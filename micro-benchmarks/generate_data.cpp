#include "storage/page/page.h"
#include <iostream>
#include <tuple>
#include "utils/utils.h"

int main(){
    Page<int64_t, int64_t, int32_t, char[4]> p{};
    print("Size of page", sizeof(p));
}
