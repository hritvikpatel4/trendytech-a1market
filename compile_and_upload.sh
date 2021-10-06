#!/usr/bin/env bash

sbt clean package

scp target/scala-2.12/*.jar itv001183@g01.itversity.com:/home/itv001183
