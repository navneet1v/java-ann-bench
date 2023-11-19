build:
	gcc -o wrapped_uring wrapped_uring.c -fPIC -luring
	gcc -shared -o libwrappeduring.so wrapped_uring.c -fPIC -luring

clean:
	rm wrapped_uring
	rm libwrappeduring.so

format:
	clang-format -i wrapped_uring.h wrapped_uring.c

tidy:
	clang-tidy wrapped_uring.h wrapped_uring.c