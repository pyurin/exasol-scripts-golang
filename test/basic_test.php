<?php

$host = '127.0.0.1';
$bucketPath = '/default/go/';
$bucketPort = '6594';
$dbPort = '8899';

function query($sql) {
    global $dbPort, $host;
    exec("./EXAplus-6.0.15/exaplus -c ".escapeshellarg("{$host}:{$dbPort}")." -x -u sys -p exasol -q -sql ".escapeshellarg($sql.';')."", $out, $result);
    print "Running sql:\n{$sql}\n\n";
    if ($result != 0) {
        print "Failed running sql:\n{$sql}\n";
        exit(1);
    }
    sleep(1);
    $out = array_slice($out, 3, count($out)-4);
    foreach ($out as $i => $row) {
        $row = preg_replace('/[ ]{3,}/', '<COL_BREAK>', $row);
        $out[$i] = array_slice(explode('<COL_BREAK>', $row), 1);
    }
    return $out;
}

// test basic exasol query
$r = query("SELECT 'result row 1'");
var_dump($r);
if ($r[[0]] != 'result row 1') {
    print("Failed basic sql");
    exit(1);
} else {
    print "Basic sql test - ok\n";
}

query("CREATE SCHEMA IF NOT EXISTS test");
query("alter system set script_languages = 'GO=localzmq+protobuf:///bfsdefault{$bucketPath}GolangImage?#buckets/bfsdefault{$bucketPath}go_entrypoint/go.sh'");
query("
CREATE OR REPLACE GO  SCALAR SCRIPT test.gotest(a DECIMAL(16,0), b DECIMAL(16,0)) EMITS (v DECIMAL(16,0), i DECIMAL(16,0)) AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {
    var sumResult int64;
    for i := *iter.ReadInt64(0); i <= *iter.ReadInt64(1); i++ {
        sumResult += i;
        iter.EmitInt64(i)
        iter.EmitInt64(sumResult)
    }
}
/
");
$r = query_rows("SELECT test.gotest(1, 3)");
if ($r != [["1","1"],["2","3"],["3","6"]]) {
    print("Failed test 1");
    exit(1);
} else {
    print "Basic test 1 - ok\n";
}



query("
CREATE OR REPLACE GO SET SCRIPT test.gotest_sum(a DECIMAL(16,0)) EMITS (v DECIMAL(16,0)) AS

package main

import \"exago\"

func Run(iter *exago.ExaIter) {
    var sumResult int64;
    for true {
        sumResult += *iter.ReadInt64(0)
        if !iter.Next() {     
            break;
        }
    }
    iter.EmitInt64(sumResult)
}
/
");
$r = query_rows("SELECT test.gotest_sum(v) FROM (SELECT 1 as v UNION ALL SELECT 10 UNION ALL SELECT 482)");
if ($r != [["493"]]) {
    print("Failed test 2");
    exit(1);
} else {
    print "Basic test 2 - ok\n";
}


print "\nAll OK\n";