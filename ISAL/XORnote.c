#include<stdio.h>
#include<stdlib.h>
#include<math.h>

int main()
{

    unsigned char c17; 
    unsigned char c8 = 162; //29 = 0001 1101
    c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);
    /*if c8 > 128 -> XOR 29. 

    Else XOR 0

    case 1 (if): c8 << 1 XOR 29 
  1 0000 0010 = 258 after shift 1 
    0001 1101 = 29
    --------------
  1 0001 1111  = 31

    case 2 (else): c8 << 1 XOR 0
    0000 1111 = 15 SHIFT 
    0001 1110 = 30
    0000 0000 = 0
    --------------

    0001 1110 = 30
    */
    printf(" c8 & 0x80: %u \n", c8 << 1);
    printf(" c17: %u ", c17);

    return 0;
}



