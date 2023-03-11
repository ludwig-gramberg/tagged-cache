<?php
namespace TaggedCache;

use Throwable;
use Webframework\DI\Attribute\Singleton;
use Webframework\Util;

#[Singleton]
class CacheService {

    const CACHE_CONTENT = 'c_'; // key -> content
    const CACHE_TAGS = 't_'; // key -> tags
    const CACHE_KEYS = 'k_'; // tag -> keys
    const CACHE_INVENTORY = 'inv'; // list of all keys

    const COMPRESSION_TYPE_GZIP = 'gz';
    const COMPRESSION_TYPE_NONE = 'rv';

    public function __construct(
        protected \Credis_Client $connection, 
        protected int $bzCompressionLevel = 7, 
        protected int $minCompressionByteSize = 2048
    ) {}

    /**
     * stores $content in cache under $key
     * if $key is not read for $expiresInSeconds it may be pruned from the cache
     */
    public function store(string $key, string $content, array $tags, int $expiresInSeconds = null): void {
        try {
            $tags = array_unique($tags);
    
            // prepare identifier
    
            $keyHash = $this->hashIdentifier($key);
    
            // get tags currently referencing this key
    
            $tagsBefore = $this->connection->sMembers(self::CACHE_TAGS.$keyHash);
    
            $tagsAdd = array_diff($tags, $tagsBefore);
            $tagsRemove = array_diff($tagsBefore, $tags);
    
            // add/remove tags for request
    
            if(!empty($tagsAdd)) {
                call_user_func_array([$this->connection, 'sAdd'], array_merge([self::CACHE_TAGS.$keyHash], $tagsAdd));
            }
            if(!empty($tagsRemove)) {
                call_user_func_array([$this->connection, 'sRem'], array_merge([self::CACHE_TAGS.$keyHash], $tagsRemove));
            }
    
            // store tag-to-keys lookup
    
            foreach($tagsAdd as $tag) {
                $tagHash = $this->hashIdentifier($tag);
                call_user_func_array([$this->connection, 'sAdd'], [self::CACHE_KEYS.$tagHash, $key]);
            }
            foreach($tagsRemove as $tag) {
                $tagHash = $this->hashIdentifier($tag);
                call_user_func_array([$this->connection, 'sRem'], [self::CACHE_KEYS.$tagHash, $key]);
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

        } catch(Throwable $t) {
            Util::logError($t);
        }
    }

    public function retrieve(string $key): ?string {
        try {
            // check if content is in inventory, if not, treat as invalid (inconsistent cache state)

            $isInInventory = $this->connection->sIsMember(self::CACHE_INVENTORY, $key);
            if(!$isInInventory) {
                return null;
            }

            // check if content exists

            $keyHash = $this->hashIdentifier($key);
            $cacheContent = $this->connection->get(self::CACHE_CONTENT.$keyHash);

            if($cacheContent === false) {
                return null;
            }

            // read first 32 characters to extract metadata

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
            
        } catch(Throwable $t) {
            Util::logError($t);
        }
        return null;
    }

    /**
     * invalidate all cache entries associated with $tags
     */
    public function invalidateTags(array $tags): void {
        try {
            // get keys affected by $tags
    
            $keys = [];
            foreach($tags as $tag) {
                $tagHash = $this->hashIdentifier($tag);
                $tagKeys = $this->connection->sMembers(self::CACHE_KEYS.$tagHash);
                $keys = array_merge($keys, $tagKeys);
            }
            $keys = array_unique($keys);
            $this->invalidateKeys($keys);
            
        } catch(Throwable $t) {
            Util::logError($t);
        }
    }

    /**
     * invalidate all cache entries associated with $tags
     */
    public function invalidateKeys(array $keys): void {
        try {
            if(empty($keys)) {
                return;
            }
            // remove content and inventory
            $contentKeys = [];
            foreach($keys as $key) {
                $contentKeys[] = self::CACHE_CONTENT.$this->hashIdentifier($key);
            }
            $this->connection->del($contentKeys);
            call_user_func_array([$this->connection, 'sRem'], array_merge([self::CACHE_INVENTORY], $keys));

        } catch(Throwable $t) {
            Util::logError($t);
        }
    }

    public function getStoredKeys(): array {
        try {
            $keys = $this->connection->sMembers(self::CACHE_INVENTORY);
            return $keys;
            
        } catch(Throwable $t) {
            Util::logError($t);
        }
        return [];
    }

    /**
     * empty the entire cache
     */
    public function flush(): void {
        try {
           $this->connection->flushAll();
        } catch(Throwable $t) {
            Util::logError($t);
        }
    }

    // ---- helpers ----

    protected function hashIdentifier($id): string {
        return sha1($id);
    }

    // ---- debug and health ----

    public function getStorageIndex(): array {
        try {
            $keys = [];
            $iterator = null;
            do {
                $resultKeys = $this->connection->scan($iterator);
                $keys = array_merge($keys, $resultKeys);
            } while($iterator <> 0);
            return $keys;            
        } catch(Throwable $t) {
            Util::logError($t);
        }
        return [];
    }

    /**
     * remove obsolete data from cache
     */
    public function clean(): void {
        try {
            $keys = [];
    
            $index = $this->getStorageIndex();
            $inventory = $this->getStoredKeys();
    
            foreach($inventory as $key) {
                if(!in_array(self::CACHE_CONTENT.$this->hashIdentifier($key), $index)) {
                    $keys[] = $key;
                }
            }
    
            foreach($index as $cacheKey) {
                $t = substr($cacheKey,0,1);
                if($t == 'k') {
                    foreach($this->connection->sMembers($cacheKey) as $key) {
                        if(!in_array($key, $inventory) || !in_array(self::CACHE_CONTENT.$this->hashIdentifier($key), $index)) {
                            $keys[] = $key;
                        }
                    }
                }
            }
            $keys = array_unique($keys);
    
            // remove from inventory and contents
    
            $this->invalidateKeys($keys);
    
            // remove from key-tag/tag-key lookups
    
            foreach($keys as $key) {
                $keyTags = $this->connection->sMembers(self::CACHE_TAGS.$this->hashIdentifier($key));
                foreach($keyTags as $tag) {
                    $this->connection->sRem(self::CACHE_KEYS.$this->hashIdentifier($tag), $key);
                }
                $this->connection->del(self::CACHE_TAGS.$this->hashIdentifier($key));
            }
    
            // remove all empty lookups
    
            foreach($index as $cacheKey) {
                $t = substr($cacheKey,0,1);
                if($t == 'k' || $t == 't') {
                    if($this->connection->sCard($cacheKey) == 0) {
                        $this->connection->del($cacheKey);
                    }
                }
            }
        } catch(Throwable $t) {
            Util::logError($t);
        }
    }

    /**
     *
     */
    public function getStorageInconsistencies(): array {
        try {
            $index = $this->getStorageIndex();
            $inventory = $this->getStoredKeys();
    
            $tags = [];
            $requests = [];
            $connections = [];
            $errors = [];
    
            foreach($inventory as $request) {
                $requests[$this->hashIdentifier($request)] = [
                    'id' => $request,
                    'has_content' => in_array(self::CACHE_CONTENT.$this->hashIdentifier($request), $index),
                    'in_inventory' => true,
                ];
            }
            foreach($index as $cacheKey) {
                $t = substr($cacheKey,0,1);
                $hash = substr($cacheKey,2);
                if($t == 't') { // tags of a request
                    foreach($this->connection->sMembers($cacheKey) as $tag) {
                        $tags[$this->hashIdentifier($tag)] = [
                            'id' => $tag,
                        ];
                    }
                } elseif($t == 'k') { // requests of a tag
                    foreach($this->connection->sMembers($cacheKey) as $request) {
                        $requestHash = $this->hashIdentifier($request);
                        if(!array_key_exists($requestHash, $requests)) {
                            $requests[$requestHash] = [
                                'id' => $request,
                                'has_content' => in_array(self::CACHE_CONTENT.$this->hashIdentifier($request), $index),
                                'in_inventory' => false,
                            ];
                        }
                    }
                }
            }
    
            // compute all connections
    
            foreach($index as $cacheKey) {
                $t = substr($cacheKey,0,1);
                $hash = substr($cacheKey,2);
                if($t == 't') { // tags of a request
                    $request = $requests[$hash];
                    $requestTags = $this->connection->sMembers($cacheKey);
                    if(!$request) {
                        $errors[] = 'severe: could not find request to tag-collection: '.implode(', ', $requestTags);
                    } else {
                        foreach($requestTags as $tag) {
                            $connectionKey = $request['id'].'-'.$tag;
                            if(!array_key_exists($connectionKey, $connections)) {
                                $connections[$connectionKey] = [
                                    'tag_to_request' => false,
                                    'request_to_tag' => true,
                                ];
                            } else {
                                $connections[$connectionKey]['request_to_tag'] = true;
                            }
                        }
                    }
                } elseif($t == 'k') { // requests of a tag
                    $tag = $tags[$hash];
                    $tagRequests = $this->connection->sMembers($cacheKey);
                    if(!$tag) {
                        $errors[] = 'severe: could not find tag to request-collection: '.implode(', ', $tagRequests);
                    } else {
                        foreach($tagRequests as $request) {
                            $connectionKey = $request.'-'.$tag['id'];
                            if(!array_key_exists($connectionKey, $connections)) {
                                $connections[$connectionKey] = [
                                    'tag_to_request' => true,
                                    'request_to_tag' => false,
                                ];
                            } else {
                                $connections[$connectionKey]['tag_to_request'] = true;
                            }
                        }
                    }
                }
            }
    
            // find problems
    
            // unidirectional connections
    
            foreach($connections as $connectionKey => $state) {
                if(!$state['tag_to_request'] || !$state['request_to_tag']) {
                    $error = 'severe: connection `'.$connectionKey.'` is not bidirectional:';
                    if(!$state['tag_to_request']) {
                        $error .= 'missing t->r';
                    }
                    if(!$state['request_to_tag']) {
                        $error .= 'missing r->t';
                    }
                    $errors[] = $error;
                }
            }
    
            // requests not in inventory
    
            foreach($requests as $state) {
                if(!$state['in_inventory']) {
                    $errors[] = 'notice: request `'.$state['id'].'` missing from inventory';
                }
            }
    
            // unaccounted keys
    
            foreach($index as $cacheKey) {
                $t = substr($cacheKey,0,1);
                $hash = substr($cacheKey,2);
                if($t == 't') { // tags of a request
                    if(!array_key_exists($hash, $requests)) {
                        $errors[] = 'warning: request-tags: '.implode(',', $this->connection->sMembers($cacheKey));
                    }
                } elseif($t == 'k') { // requests of a tag
                    if(!array_key_exists($hash, $tags)) {
                        $errors[] = 'warning: tag-requests: '.implode(',', $this->connection->sMembers($cacheKey));
                    }
                }
            }
    
            return $errors;
            
        } catch(Throwable $t) {
            Util::logError($t);
        }
        return [];
    }

}