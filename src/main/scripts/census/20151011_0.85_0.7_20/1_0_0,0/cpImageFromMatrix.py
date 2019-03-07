'''
Created on Aug 25, 2015

@author: ameya
'''

import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
import os

def main(argv):
    if len(argv) < 2:
        print("Usage : {} <matrix-file> [gray:0/1] [interpolation-value]".format(argv[0]))
        exit(-1)
    
    matrixFile = argv[1]

    baseName = os.path.splitext(matrixFile)[0]
    
    title = [x for x in baseName.replace(',','_').split('_') if not x.isdigit()]

    grayScale = False 
    if len(argv) >= 3:
        grayScale = bool(int(argv[2]))

    grayScale = False  

    print("grayScale : {}".format(grayScale))
    
    interpolationValue = 'bicubic'
    if len(argv) == 4:
        interpolationValue = argv[3]

    # load data
    array = np.loadtxt(matrixFile, delimiter = ',')

    print("Original shape : {}".format(array.shape))

    array = array[-10:,]
    
    print("Trimmed shape : {}".format(array.shape))
    # get array dimensions
    (rows, cols) = array.shape

    # max and min value in the array

    maxValue = np.amax(array)
    minValue = np.amin(array)
    midValue = (maxValue + minValue) / 2
    cbarTickValues = [minValue, 
            (minValue + midValue) / 2, 
            midValue, 
            (maxValue + midValue) / 2, 
            maxValue]
    
    mainFigure = plt.figure()
    axes = mainFigure.add_subplot(111)

    # ticks for the main image
    majorTicksY = np.arange(0, rows, 1)
    majorTicksX = np.arange(0, cols, 1)
    

    #axes = plt.axes()

    # ticks for grid lines
    axes.set_xticks(majorTicksX + 1)
    axes.set_yticks(majorTicksY + 1)

    # blank out labels on the image
    axes.xaxis.set_ticklabels([])
    axes.yaxis.set_ticklabels([])

    # swith on grid.
    axes.grid(b=True, which='major', color='k', linestyle='-')
    
    #axes.set_title(' '.join(title))

    if grayScale or 'UMatrix' in title: 
        image = axes.imshow(array, aspect = 'equal', interpolation = interpolationValue,
            cmap = plt.get_cmap('gray_r'), origin = 'upper', extent = [0, cols, rows,0]) #[0,cols, 0, rows]) # pylab.gray())
        '''
        image = plt.matshow(array, aspect = 'equal', interpolation = interpolationValue,
            cmap = plt.get_cmap('gray_r'), origin = 'upper', extent = [0, cols, rows,0]) #[0,cols, 0, rows]) # pylab.gray())
        '''
    else :
        image = axes.imshow(array, aspect = 'equal', interpolation = interpolationValue,
            origin = 'upper', extent = [0, cols, rows, 0]) # pylab.gray())

    cbar = mainFigure.colorbar(image, orientation='vertical', 
            ticks = cbarTickValues,shrink=1, pad=0.08) 

    cbar.ax.set_yticklabels([str(round(x,2)) for x in cbarTickValues])
    
    '''
    tickLocator = ticker.MaxNLocator(nbins = 5, prune = None)
    cbar.locator = tickLocator
    #cbar.ax.get_yaxis().get_major_formatter().set_useOffset(False)
    #cbar.ax.get_yaxis().set_major_locator(ticker.AutoLocator())
    cbar.update_ticks()
    '''
    
    #plt.show(block = True)
    #fig = plt.gcf()
    mainFigure.tight_layout()
    mainFigure.savefig("{}.png".format(baseName), bbox_inches='tight')
    
    print("Output file : {}.png".format(baseName))
    
if __name__ == '__main__':
    main(sys.argv)
