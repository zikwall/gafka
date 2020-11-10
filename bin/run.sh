#!/bin/bash

run -d -p 3000:3000 --env-file .env --name gafka-server qwx1337/gafka:latest