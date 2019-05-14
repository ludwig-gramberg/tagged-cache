<?php
namespace TaggedCache;

class CacheService {

    const CACHE_CONTENT = 'c_'; // key -> content
    const CACHE_TAGS = 't_'; // key -> tags
    const CACHE_KEYS = 'k_'; // tag -> keys
    const CACHE_INVENTORY = 'inv'; // list of all keys

    const COMPRESSION_TYPE_GZIP = 'gz';
    const COMPRESSION_TYPE_NONE = 'rv';

    /**
     * @var \Credis_Client
     */
    protected $connection;

    /**
     * @var int
     */
    protected $minCompressionByteSize;

    /**
     * @var int
     */
    protected $bzCompressionLevel;

    /**
     * CacheService constructor.
     * @param \Credis_Client $redisConnection
     * @param int $bzCompressionLevel
     * @param int $minCompressionByteSize
     */
    public function __construct(\Credis_Client $redisConnection, $bzCompressionLevel = 7, $minCompressionByteSize = 2048) {
        $this->connection = $redisConnection;
        $this->bzCompressionLevel = $bzCompressionLevel;
        $this->minCompressionByteSize = $minCompressionByteSize;
    }

    /**
     * stores $content in cache under $key
     * if $key is not read for $expiresInSeconds it may be pruned from the cache
     *
     * @param string $key
     * @param string $content
     * @param array $tags
     * @param int|null $expiresInSeconds
     */
    public function store($key, $content, array $tags, $expiresInSeconds = null) {

        // prepare identifier

        $keyHash = $this->hashIdentifier($key);

        // overwrite key-to-tags lookup

        $this->connection->del(self::CACHE_TAGS.$keyHash);
        call_user_func_array([$this->connection, 'sAdd'], array_merge([self::CACHE_TAGS.$keyHash], $tags));

        // store tag-to-keys lookup

        foreach($tags as $tag) {
            $tagHash = $this->hashIdentifier($tag);
            call_user_func_array([$this->connection, 'sAdd'], [self::CACHE_KEYS.$tagHash, $key]);
        }

        // store content

        $cacheContent = self::COMPRESSION_TYPE_NONE.':'.$content; // rv: raw value

        // compression

        if(strlen($content) > $this->minCompressionByteSize) { // don't compress if too small
            $compressedContent = gzcompress($content, $this->bzCompressionLevel);
            if($compressedContent !== false) {
                $cacheContent = self::COMPRESSION_TYPE_GZIP.':'.$compressedContent; // gz: compressed
            }
        }

        $cacheContent = $expiresInSeconds.':'.$cacheContent;

        // example of cached format: TTL:COMPRESSION_TYPE:CONTENT
        // 86400:rv:some_cached_content

        $options = [];
        if($expiresInSeconds !== null) {
            $options['EX'] = $expiresInSeconds;
        }

        $this->connection->set(self::CACHE_CONTENT.$keyHash, $cacheContent, $options);

        // add page to inventory

        $this->connection->sAdd(self::CACHE_INVENTORY, $key);
    }

    /**
     * @param string $key
     * @return string|null
     */
    public function retrieve($key) {

        $keyHash = $this->hashIdentifier($key);
        $cacheContent = $this->connection->get(self::CACHE_CONTENT.$keyHash);

        if($cacheContent === false) {
            return null;
        }

        // check if content is in inventory, if not, treat as invalid (inconsistent cache state)

        $isInInventory = $this->connection->sIsMember(self::CACHE_INVENTORY, $key);
        if(!$isInInventory) {
            return null;
        }

        // read first 32 characters to extract meta data

        $metaData = substr($cacheContent, 0, 32);
        if(!preg_match('/^([0-9]*):('.self::COMPRESSION_TYPE_GZIP.'|'.self::COMPRESSION_TYPE_NONE.'):/', $metaData, $m)) {
            return null;
        }

        $expiresInSeconds = $m[1] == '' ? null : intval($m[1]);
        $compressionType = $m[2];
        $metaCharacters = strlen($m[0]);

        $cacheContent = substr($cacheContent, $metaCharacters);

        if($compressionType == self::COMPRESSION_TYPE_GZIP) {
            $cacheContent = gzuncompress($cacheContent);
            if($cacheContent === false) {
                return null;
            }
        }

        // trigger re-expire on successful read if expire time is defined

        if($expiresInSeconds !== null) {

            $this->connection->expire($key, $expiresInSeconds);
        }

        return $cacheContent;
    }

    /**
     * invalidate all cache entries associated with $tags
     *
     * @param array $tags
     */
    public function invalidateTags(array $tags) {

        // get keys affected by $tags

        $keys = [];
        foreach($tags as $tag) {
            $tagHash = $this->hashIdentifier($tag);
            $tagKeys = $this->connection->sMembers(self::CACHE_KEYS.$tagHash);
            $keys = array_merge($keys, $tagKeys);
        }
        $keys = array_unique($keys);

        $this->cleanInvalidated($tags, $keys);
    }

    /**
     * invalidate all cache entries associated with $tags
     *
     * @param array $keys
     */
    public function invalidateKeys(array $keys) {

        // get tags for keys

        $tags = [];
        foreach($keys as $key) {
            $keyHash = $this->hashIdentifier($key);
            $keyTags = $this->connection->sMembers(self::CACHE_TAGS.$keyHash);
            $tags = array_merge($tags, $keyTags);
        }
        $tags = array_unique($tags);

        $this->cleanInvalidated($tags, $keys);
    }

    /**
     * @return array
     */
    public function getStoredKeys() {
        $keys = $this->connection->sMembers(self::CACHE_INVENTORY);
        return $keys;
    }

    /**
     * empty the entire cache
     */
    public function flush() {
        $this->connection->flushAll();
    }

    // ---- helpers ----

    protected function cleanInvalidated(array $tags, array $keys) {
        if(empty($keys)) {
            return;
        }

        // delete content entries and key-to-tags entries

        $delete = [];
        foreach($keys as $key) {
            $keyHash = $this->hashIdentifier($key);
            $delete[] = self::CACHE_TAGS.$keyHash;
            $delete[] = self::CACHE_CONTENT.$keyHash;
        }

        call_user_func_array([$this->connection, 'del'], $delete);

        // remove invalidated keys from tag-sets

        foreach($tags as $tag) {
            $tagHash = $this->hashIdentifier($tag);
            call_user_func_array([$this->connection, 'sRem'], array_merge([self::CACHE_KEYS.$tagHash], $keys));
        }

        // remove invalidated keys from inventory

        call_user_func_array([$this->connection, 'sRem'], array_merge([self::CACHE_INVENTORY], $keys));
    }

    protected function hashIdentifier($id) {
        return sha1($id);
    }

}