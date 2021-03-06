import sys
# import threading
import multiprocessing
import time

"""
Using Multiprocessing instead of threading:
Separate memory space
Takes advantage of multiple CPUs & cores
Avoids GIL limitations for cPython
Eliminates most needs for synchronization primitives unless if you use shared memory (instead, it's more of a communication model for IPC)
Child processes are interruptible/killable
Python multiprocessing module includes useful abstractions with an interface much like threading.Thread
A must with cPython for CPU-bound processing
"""

itera = 1000000  # 64 ** 3  # 64 is beacuse it can be a division of 1, 2, 4, 8


def flops(num_of_thread):
    """
    floating point operations per second
    Args:
        num_of_thread
    """
    nums = [1.3, 10.5, 12.1, 30.4]
    num = 0
    # for i in range(int(itera/num_of_thread)): # after confirming with TA each thread needs to have the same oper
    for i in range(int(itera)):
        num = nums[1] + nums[2] + nums[3]
        num = nums[1] * nums[2] + nums[3]


def iops(num_of_thread):
    """
    integer operations per second
    Args:
        num_of_thread
    """
    nums = [5, 20, 12, 30]
    num = 0
    # for i in range(int(itera/num_of_thread)):
    for i in range(int(itera)):
        num = nums[1] + nums[2] + nums[3]
        num = nums[1] * nums[2] + nums[3]


def createAndRunThread(num_of_thread, data_type):
    """
    Create and run the thread
    Args:
        num_of_thread
        data_type
    Returns:
        Nothing
    """
    list_of_t = []
    if data_type == 'flops':
        for i in range(num_of_thread):
            # t = threading.Thread(target=flops, args=(num_of_thread,))
            t = multiprocessing.Process(target=flops, args=(num_of_thread,))
            list_of_t.append(t)
            t.start()
    elif data_type == 'iops':
        for i in range(num_of_thread):
            # t = threading.Thread(target=iops, args=(num_of_thread,))
            t = multiprocessing.Process(target=iops, args=(num_of_thread,))
            list_of_t.append(t)
            t.start()

    for t in list_of_t:
        t.join()


def main():
    """
    argv 1:number of thread argv 2: iops or flops, note:0 is script
    thread: 1, 2, 4, 8
    operation: iops, flops
    """
    if len(sys.argv) > 3:  # error handling
        print('Error: invalid arguments')
    else:
        try:
            num_of_thread = int(sys.argv[1])
            data_type = sys.argv[2]
        except:
            num_of_thread = 2
            data_type = "flops"

        start = time.time()  # start timer
        createAndRunThread(num_of_thread, data_type)
        duration = time.time() - start
        print('Time elapsed', duration)
        ops = itera / duration
        gops = ops / (10 ** 9)  # For Giga
        if data_type == 'flops':
            print('Giga FLOPS:', gops)
        else:
            print('Giga ILOPS:', gops)


if __name__ == '__main__':
    main()
