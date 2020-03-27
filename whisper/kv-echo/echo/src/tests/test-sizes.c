#include <stdio.h>
#include <stdint.h>

int main(int argc, char *argv[])
{
	printf("sizeof size_t: %u\n", sizeof(size_t));
	printf("sizeof int: %u\n", sizeof(int));
	printf("sizeof long int: %u\n", sizeof(long int));
	printf("sizeof long long int: %u\n", sizeof(long long int));
	printf("sizeof unsigned int: %u\n", sizeof(unsigned int));
	printf("sizeof unsigned long int: %u\n", sizeof(unsigned long int));
	printf("sizeof unsigned long long int: %u\n", sizeof(unsigned long long int));
	printf("sizeof uint32_t: %u\n", sizeof(uint32_t));
	printf("sizeof uint64_t: %u\n", sizeof(uint64_t));
	printf("sizeof void *: %u\n", sizeof(void *));

	return 0;
}
