#!/bin/bash

rm -fr *.pem

openssl genrsa -out key.pem 1024
openssl req -new -key key.pem -out request.pem
openssl x509 -req -days 3650 -in request.pem -signkey key.pem -out certificate.pem

chmod 400 key.pem
chmod 400 certificate.pem


#EOF
