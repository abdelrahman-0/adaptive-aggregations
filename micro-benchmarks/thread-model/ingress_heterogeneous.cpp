#pragma once

int main(){
    // setup connection

    // create network threads
    // each network thread creates num_workers * 2 many buffer pages
    // each network thread spins
    // then fills SPMC queue until "done"

    // create worker threads
    // each worker thread spins
    // then consumes from associated SPMC until done
}
