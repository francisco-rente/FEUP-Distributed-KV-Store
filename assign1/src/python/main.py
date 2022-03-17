import sys 
import output
from matrixproduct import * 

# Usage 
# python main.py <option 1-3> <size> [blksize when opt 3] [floating point?]


def main(option, dim, block): 
    if(option < 1 or option > 3): 
        raise Exception("Invalid option")
    elif(dim < 0): 
        raise Exception("Invalid dimension")
    
    m_a = generate_matrixA(dim)
    m_b = generate_matrixB(dim)
    
    if(option == 3 and dim % block != 0):
        print(dim % block, dim, block)
        raise Exception("Invalid block size")


    if(option == 1): 
        OnMult(m_a, m_b, dim, dim)
    elif (option == 2): 
        OnMultLine(m_a, m_b, dim, dim)
    else: 
        if(block != -1):
            OnMultBlock(m_a, m_b, dim, dim, block)
        else:
            raise Exception("Invalid block size")

if __name__ == "__main__": 
    if(len(sys.argv)> 4): 
        raise Exception("Invalid command")

    block = int(sys.argv[3]) if (len(sys.argv) == 4) else -1

    main(int(sys.argv[1]), int(sys.argv[2]), block)