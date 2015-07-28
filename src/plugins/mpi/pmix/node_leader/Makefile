
all: clean
	gcc -c ln_leader.c -g
	gcc -c fileops.c -g
	gcc -g -o main ln_leader.o fileops.o

clean:
	rm -f ln_leader.o main
	rm -f fileops.o

