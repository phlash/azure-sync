# Build slice program

FLAGS=$(shell pkg-config --cflags --libs libcrypto)

slice: slice.c
	gcc -o $@ $< $(FLAGS)
