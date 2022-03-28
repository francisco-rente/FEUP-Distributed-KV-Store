from time import perf_counter, perf_counter_ns
from output import output_to_file


def gflops(size, time): 
    return 2 * (size)**3 / time


def generate_matrixA(m_ar):
    return [1.0 for x in range(0, m_ar ** 2)]


def generate_matrixB(m_br):
    matrixB = [0 for x in range(0, m_br ** 2)]

    for i in range(0, m_br):
        for j in range(0, m_br):
            matrixB[i*m_br + j] = (i+1.0)

    return matrixB


def OnMult(m_a, m_b, m_ar, m_br):
    matrixC = [0 for x in range(0, min(m_ar, m_br)**2)]

    start = perf_counter()

    for i in range(0, m_ar):
        for j in range(0, m_br):
            temp = 0
            for k in range(0, m_ar):
                temp += m_a[i * m_ar + k] * m_b[k * m_br + j] 
            matrixC[i * m_ar + j] = temp

    end = perf_counter()

    time = end - start

    print("time elapsed ", time, "s", "GFLOPS: ", gflops(m_ar, time))

    output_to_file(1, m_ar, time)

    return matrixC


def OnMultLine(m_a, m_b, m_ar, m_br):
    matrixC = [0 for x in range(0, min(m_ar, m_br)**2)]

    start = perf_counter()

    for i in range(0, m_ar):
        for k in range(0, m_ar):
            for j in range(0, m_br):
                matrixC[i*m_ar+j] += m_a[i*m_ar+k] * m_b[k*m_br+j]

    end = perf_counter()

    time = end - start

    print("time elapsed ", time, "s", "GFLOPS: ", gflops(m_ar, time))

    output_to_file(2, m_ar, time)
    print_matrix(matrixC, m_ar)
    return matrixC


def OnMultBlock(m_a, m_b, m_ar, m_br, blkSize):
    matrixC = [0 for x in range(0, min(m_ar, m_br)**2)]
    n_blocks = min(m_ar, m_br) // blkSize

    start = perf_counter()

    # print_matrix(m_a, m_ar)
    # print_matrix(m_b, m_br) 
    # print_matrix(matrixC, m_ar)


    for ii in range(0, m_ar, blkSize): 
        for kk in range(0, m_br, blkSize): 
            for jj in range(0, m_br, blkSize): 
                for i in range(ii, ii + blkSize): 
                    for k in range(kk, kk + blkSize):
                        for j in range(jj, jj + blkSize):  
                            matrixC[i * m_ar + j] += m_a[i*m_ar+k] * m_b[k*m_br+j]

    end = perf_counter()

    time = end - start

    print("time elapsed: ", time, "s ", "GFLOPS: ", gflops(m_ar, time))

    output_to_file(3, m_ar, time, blkSize)
    print_matrix(matrixC, m_ar)
    
    return matrixC


def print_matrix(matrix, dim):
    for i in range(0, dim):
        print("[", end="")
        for j in range(0, dim):
            print(str(matrix[i * dim + j]), end="")
            if(j != dim - 1):
                print(", ", end="")
        print("]")
    print("----------")


# How to calculate GigaFloops: 2n³/tempo    -> n: operatios
