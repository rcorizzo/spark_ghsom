'''
Created on Aug 25, 2015

@author: ameya
'''

import sys
import numpy as np
import matplotlib.pyplot as plt
import os

def getMatrixFromFile(filename) :
    matrix = list()
    with open(filename, 'r') as f : 
        for line in f : 
            rowList = list()
            values = line.split('|')

def main(argv):
    if len(argv) < 2:
        print("Usage : {} <matrix-file> [gray:0/1]".format(argv[0]))
        exit(-1)
    
    matrixFile = getMatrixFromFile(argv[1])

    baseName = os.path.splitext(matrixFile)[0]
    
    grayScale = False 
    if len(argv) == 3:
        grayScale = bool(argv[2])
    
    array = np.loadtxt(matrixFile, delimiter = ',')
    
    (rows, cols) = array.shape
    
    majorTicksY = np.arange(0, rows, 1)
    majorTicksX = np.arange(0, cols, 1)
    
    plt.figure(1, figsize=(8, 6), dpi=80)
    axes = plt.axes()

    axes.set_xticks(majorTicksX + 1)
    axes.set_yticks(majorTicksY + 1)

    plt.grid(b=True, which='major', color='k', linestyle='-')
    
    plt.title(baseName)

    if grayScale : 
        plt.imshow(array, aspect = 'auto', interpolation='none',
            cmap = plt.get_cmap('gray_r'), origin = 'lower', extent = [0,cols, 0, rows]) # pylab.gray())
    else :
        plt.imshow(array, aspect = 'auto', interpolation='none',
            origin = 'lower', extent = [0,cols, 0, rows]) # pylab.gray())

    plt.colorbar( orientation='horizontal' )
    fig = plt.gcf()
    fig.savefig("{}.png".format(baseName))
    print("Output file : {}.png".format(baseName))
    
    
if __name__ == '__main__':
    main(sys.argv)
