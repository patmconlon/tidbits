/***************************************************
* Name: fibonacci.cpp
* Author: Patrick Conlon
*
* Description:
* Does the Fibonacci sequence.
***************************************************/
#include <iostream>

using namespace std;

int main() {
    int a = 0; //low number
    int b = 1; //high number

   //Loop till done
    do {
      //b = 1 + 0
        b += a;
      //a = 1 - 0
        a = b - a;

      cout << a << "\n";
    }
   while( b < 1000000000 );

    return 0;
}
