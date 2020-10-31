#!/bin/bash

sed '/^paxos Dial\(\)/d' < $1 > tmpout; mv tmpout $1
sed '/^read unix/d' < $1 > tmpout; mv tmpout $1
sed '/^write unix/d' < $1 > tmpout; mv tmpout $1
sed '/^unexpected EOF/d' < $1 > tmpout; mv tmpout $1
sed '/^2020\/05\/27/d' < $1 > tmpout; mv tmpout $1
