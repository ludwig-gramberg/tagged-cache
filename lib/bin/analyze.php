<?php
namespace TaggedCache;

require_once '../../../autoload.php';

$usageHelp = <<<HELP

usage: php dump.php [-h] [-p] [-a]
  -h redis-host defaults to 127.0.0.1
  -p redis-port defaults to 6379
  -a redis-auth
  --help show this help 

HELP;

// options

$options = getopt('hpa::', ['help']);
if(array_key_exists('help', $options)) {
    echo $usageHelp;
    exit;
}

$host = array_key_exists('h', $options) ? $options['h'] : '127.0.0.1';
$port = array_key_exists('p', $options) ? $options['p'] : 6379;
$auth = array_key_exists('a', $options) ? $options['a'] : null;

$connection = new \Credis_Client($host, $port, null, '', 0, $auth);
$cacheService = new CacheService($connection);

$errors = $cacheService->getStorageInconsistencies();
if(empty($errors)) {
    echo "cache is consistent\n";
    exit;
}

echo "the following inconsistencies were detected\n";
foreach($errors as $error) {
    echo "\t$error\n";
}