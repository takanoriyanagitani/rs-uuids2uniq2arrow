#!/bin/sh

input(){
	printf '%s\n' \
		0123-4567-89ab-cdef \
		0123-4567-89ab-cdef \
		cafe-f00d-beaf-face
}

input |
	xxd -r -ps |
	./uuids2uniq2arrow
