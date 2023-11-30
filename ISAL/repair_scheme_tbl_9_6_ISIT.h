unsigned char parity[256];

unsigned char h_htbl[9][9][9] = 
{ 
{ 	  { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
	  { 4, 113, 236, 186, 104, 0, 0, 0, 0 }, 
      { 4, 113, 124, 198, 205, 0, 0, 0, 0 }, 
      { 6, 212, 144, 109, 111, 198, 82, 0, 0 }, 
      { 4, 78, 111, 121, 159, 0, 0, 0, 0 }, 
	  { 2, 124, 205, 0, 0, 0, 0, 0, 0 }, 
      { 4, 38, 236, 77, 25, 0, 0, 0, 0 }, 
	  { 4, 111, 132, 86, 171, 0, 0, 0, 0 }, 
      { 4, 147, 132, 75, 86, 0, 0, 0, 0 } }, 
  { { 2, 38, 158, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 124, 104, 205, 254, 0, 0, 0, 0 }, { 4, 56, 124, 205, 33, 0, 0, 0, 0 },
      { 4, 176, 218, 40, 224, 0, 0, 0, 0 }, { 4, 65, 27, 75, 89, 0, 0, 0, 0 }, 
      { 4, 32, 141, 82, 12, 0, 0, 0, 0 }, { 4, 80, 218, 18, 224, 0, 0, 0, 0 }, 
      { 6, 36, 80, 181, 198, 18, 81, 0, 0 } }, 
  { { 6, 56, 28, 152, 147, 197, 224, 0, 0 }, { 4, 113, 38, 36, 184, 0, 0, 0, 0 },
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 2, 183, 11, 0, 0, 0, 0, 0, 0 }, 
      { 4, 76, 38, 112, 200, 0, 0, 0, 0 }, { 6, 16, 136, 38, 73, 198, 184, 0, 0 }
        , { 4, 113, 147, 4, 207, 0, 0, 0, 0 }, { 2, 109, 148, 0, 0, 0, 0, 0, 0 },
      { 4, 109, 148, 200, 254, 0, 0, 0, 0 } }, 
  { { 4, 218, 23, 229, 153, 0, 0, 0, 0 }, { 6, 105, 198, 25, 85, 245, 252, 0, 0 }
        , { 4, 116, 154, 15, 143, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 6, 196, 28, 35, 69, 85, 250, 0, 0 }, { 4, 248, 25, 245, 143, 0, 0, 0, 0 }
        , { 4, 71, 139, 193, 252, 0, 0, 0, 0 }, 
      { 4, 118, 183, 250, 125, 0, 0, 0, 0 }, 
      { 4, 145, 73, 118, 250, 0, 0, 0, 0 } }, 
  { { 2, 9, 83, 0, 0, 0, 0, 0, 0 }, { 2, 218, 242, 0, 0, 0, 0, 0, 0 }, 
      { 4, 238, 113, 78, 203, 0, 0, 0, 0 }, 
      { 6, 113, 56, 14, 118, 216, 108, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 145, 14, 78, 108, 0, 0, 0, 0 }, { 4, 76, 183, 60, 11, 0, 0, 0, 0 }, 
      { 4, 226, 116, 227, 131, 0, 0, 0, 0 }, { 4, 29, 183, 216, 11, 0, 0, 0, 0 } 
     }, 
  { { 6, 58, 136, 56, 95, 159, 254, 0, 0 }, { 4, 36, 227, 131, 87, 0, 0, 0, 0 }, 
      { 4, 218, 104, 253, 64, 0, 0, 0, 0 }, { 4, 113, 27, 185, 24, 0, 0, 0, 0 }, 
      { 6, 136, 152, 147, 36, 180, 224, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 106, 194, 75, 162, 0, 0, 0, 0 }, 
      { 6, 129, 36, 4, 141, 172, 254, 0, 0 }, { 4, 136, 38, 73, 95, 0, 0, 0, 0 } 
     }, 
  { { 6, 26, 238, 38, 36, 85, 61, 0, 0 }, { 4, 58, 176, 226, 117, 0, 0, 0, 0 }, 
      { 4, 29, 183, 216, 188, 0, 0, 0, 0 }, { 4, 16, 3, 183, 27, 0, 0, 0, 0 }, 
      { 2, 9, 83, 0, 0, 0, 0, 0, 0 }, { 4, 58, 176, 226, 151, 0, 0, 0, 0 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 6, 113, 116, 181, 97, 149, 143, 0, 0 }, 
      { 4, 116, 105, 97, 216, 0, 0, 0, 0 } }, 
  { { 4, 217, 104, 165, 254, 0, 0, 0, 0 }, { 6, 136, 56, 169, 247, 171, 1, 0, 0 }
        , { 4, 218, 109, 148, 242, 0, 0, 0, 0 }, 
      { 4, 116, 13, 111, 122, 0, 0, 0, 0 }, { 4, 109, 123, 251, 2, 0, 0, 0, 0 }, 
      { 4, 247, 172, 254, 1, 0, 0, 0, 0 }, { 4, 141, 180, 12, 171, 0, 0, 0, 0 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 4, 109, 20, 249, 2, 0, 0, 0, 0 } }, 
  { { 4, 51, 104, 234, 254, 0, 0, 0, 0 }, { 4, 192, 73, 198, 174, 0, 0, 0, 0 }, 
      { 6, 113, 36, 74, 12, 252, 254, 0, 0 }, { 4, 145, 7, 180, 167, 0, 0, 0, 0 }
        , { 4, 183, 74, 188, 252, 0, 0, 0, 0 }, 
      { 6, 26, 238, 56, 73, 216, 174, 0, 0 }, 
      { 6, 136, 56, 218, 216, 37, 40, 0, 0 }, 
      { 4, 214, 150, 169, 20, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 } } 
};

unsigned char h_RTable[9][9][9] =
{ 
{ { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 4, 0, 0, 8, 2, 0, 0, 11, 7 }, 
      { 4, 4, 1, 2, 15, 5, 4, 3, 10 }, { 6, 32, 48, 49, 4, 13, 34, 49, 4 }, 
      { 4, 0, 0, 4, 2, 5, 10, 13, 3 }, { 2, 0, 0, 2, 1, 3, 2, 2, 1 }, 
      { 4, 0, 0, 1, 5, 0, 0, 2, 12 }, { 4, 4, 2, 8, 7, 0, 0, 8, 7 }, 
      { 4, 4, 1, 3, 10, 0, 0, 4, 1 } }, 
  { { 2, 0, 0, 1, 3, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 8, 2, 14, 3, 0, 0, 2, 10 }, { 4, 4, 2, 14, 1, 0, 0, 0, 0 }, 
      { 4, 0, 0, 4, 2, 0, 0, 12, 15 }, { 4, 2, 10, 0, 0, 0, 0, 3, 5 }, 
      { 4, 4, 1, 3, 11, 3, 11, 11, 8 }, { 4, 2, 10, 4, 7, 13, 11, 4, 7 }, 
      { 6, 2, 18, 8, 1, 4, 45, 55, 56 } }, 
  { { 6, 1, 8, 32, 10, 47, 21, 12, 52 }, { 4, 1, 4, 8, 7, 14, 12, 3, 10 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 2, 0, 0, 0, 0, 0, 0, 1, 2 }, 
      { 4, 2, 8, 0, 0, 3, 4, 0, 0 }, { 6, 1, 8, 24, 4, 11, 56, 1, 8 }, 
      { 4, 0, 0, 1, 5, 6, 12, 0, 0 }, { 2, 1, 3, 3, 2, 3, 2, 0, 0 }, 
      { 4, 4, 12, 12, 8, 2, 5, 0, 0 } }, 
  { { 4, 0, 0, 1, 5, 6, 8, 15, 3 }, { 6, 2, 10, 40, 12, 45, 18, 23, 33 }, 
      { 4, 2, 4, 0, 0, 1, 14, 5, 8 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 6, 8, 4, 10, 11, 42, 53, 9, 14 }, { 4, 2, 6, 6, 4, 1, 12, 0, 0 }, 
      { 4, 8, 4, 0, 0, 0, 0, 2, 13 }, { 4, 2, 8, 0, 0, 9, 4, 8, 10 }, 
      { 4, 1, 2, 2, 3, 8, 13, 8, 13 } }, 
  { { 2, 1, 2, 0, 0, 2, 3, 2, 3 }, { 2, 0, 0, 0, 0, 1, 2, 1, 2 }, 
      { 4, 0, 0, 1, 4, 2, 14, 0, 0 }, { 6, 8, 1, 8, 1, 41, 3, 44, 25 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 4, 0, 0, 4, 1, 2, 14, 0, 0 }, 
      { 4, 1, 4, 5, 1, 12, 7, 0, 0 }, { 4, 0, 0, 1, 3, 10, 15, 10, 15 }, 
      { 4, 0, 0, 1, 4, 9, 6, 9, 6 } }, 
  { { 6, 4, 16, 8, 44, 40, 52, 54, 5 }, { 4, 2, 6, 7, 12, 0, 0, 0, 0 }, 
      { 4, 2, 1, 7, 12, 6, 15, 6, 15 }, { 4, 1, 5, 7, 15, 7, 15, 13, 3 }, 
      { 6, 1, 16, 9, 13, 26, 48, 29, 21 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 1, 9, 0, 0, 9, 8, 4, 7 }, { 6, 0, 0, 32, 4, 8, 34, 5, 21 }, 
      { 4, 0, 0, 1, 8, 10, 7, 0, 0 } }, 
  { { 6, 32, 16, 49, 51, 54, 27, 37, 9 }, { 4, 4, 12, 1, 3, 14, 9, 0, 0 }, 
      { 4, 0, 0, 0, 0, 8, 2, 6, 11 }, { 4, 4, 1, 2, 11, 4, 1, 4, 1 }, 
      { 2, 2, 3, 0, 0, 0, 0, 0, 0 }, { 4, 4, 12, 0, 0, 13, 10, 14, 11 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, { 6, 4, 20, 18, 19, 14, 41, 0, 0 }, 
      { 4, 2, 10, 0, 0, 2, 10, 1, 13 } }, 
  { { 4, 1, 3, 3, 2, 11, 5, 0, 0 }, { 6, 4, 1, 0, 0, 8, 36, 2, 19 }, 
      { 4, 1, 8, 5, 10, 0, 0, 0, 0 }, { 4, 4, 5, 0, 0, 13, 10, 0, 0 }, 
      { 4, 2, 6, 6, 4, 3, 8, 1, 14 }, { 4, 8, 1, 2, 12, 0, 0, 2, 12 }, 
      { 4, 2, 10, 1, 13, 11, 5, 13, 12 }, { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 
      { 4, 2, 8, 0, 0, 0, 0, 1, 6 } }, 
  { { 4, 2, 10, 0, 0, 0, 0, 6, 5 }, { 4, 4, 5, 5, 1, 2, 10, 11, 12 }, 
      { 6, 2, 10, 4, 38, 60, 9, 27, 28 }, { 4, 0, 0, 2, 1, 4, 9, 11, 12 }, 
      { 4, 1, 5, 3, 15, 11, 13, 13, 6 }, { 6, 4, 5, 37, 17, 53, 33, 6, 40 }, 
      { 6, 8, 1, 11, 43, 26, 5, 39, 55 }, { 4, 4, 1, 0, 0, 0, 0, 12, 3 }, 
      { 0, 0, 0, 0, 0, 0, 0, 0, 0 } } 
};

unsigned char h_dtbl[9][8] = 
{     
    { 78, 147, 212, 102, 26, 174, 184, 155 }, 
    { 129, 141, 197, 81, 107, 229, 10, 79 },
    { 36, 160, 25, 201, 100, 3, 82, 243 }, 
    { 97, 170, 199, 224, 154, 245, 51, 89 },
    { 168, 122, 63, 216, 56, 192, 116, 226 }, 
    { 10, 79, 233, 15, 3, 103, 134, 149 },
    { 242, 119, 182, 171, 180, 26, 78, 147 }, 
    { 123, 4, 33, 9, 124, 28, 242, 119 },
    { 224, 39, 117, 52, 223, 255, 96, 124 } ,
};


