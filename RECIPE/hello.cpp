#include <stdio.h>

int func(){
	int a=2*2;
	return a;
}

int func2(){
	int a=1;
	{
		int b = a+2;
		a = b+a;
	}
	{
		int c = a - 2;
		a = a - c;
	}

	return a * func();
}

int main() {
  printf("hello world\n");
  func2();
  return 0;
}
