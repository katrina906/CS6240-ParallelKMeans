import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# read within sum of squares error that was outputted for each K by KMeans program (parallel k)
def readInput(inputDir):
    wSoSList = []
    # loop through all files in input directory
    for filename in os.listdir(inputDir):
        with open(os.path.join(inputDir, filename), 'rb') as f:
            wSoS = f.read()
            # if file of correct format (don't include _SUCCESS etc.)
            if b'Array' in wSoS:
                # format into list of lists of format [[k, wSoS], [k, wSoS], ...]
                wSoS = wSoS.decode('utf8')\
                    .replace('Array', '').replace(')', '').replace('(', '')\
                    .split('\n')
                wSoS = [i.split(',') for i in wSoS]
                wSoS = [[float(i) for i in sublist if i != ''] for sublist in wSoS]
                wSoSList.append(wSoS)

    # flatten list of lists (one sublist per input file)
    wSoSList = [item for sublist in wSoSList for item in sublist if item != []]

    return wSoSList

# read within sum of squares error that was outputted for each K by KMeans program (multi k)
def readMultiKInput(inputDir):
    wSoSList = []
    # loop through all files in input directory
    for filename in os.listdir(inputDir):
        with open(os.path.join(inputDir, filename), 'rb') as f:
            # MultiK output is all on one line, so each line is a whole K-Means clustering
            data = f.read()
            if b'(Setup(' in data:
                data = data.decode('utf8')
                lines = data.split('\n')
                for line in lines:
                    # Skip blank terminating line
                    if len(line) > 1:
                        # Cluster value K and the wSoS error both occur before "ListBuffer" appears
                        end = line.find("ListBuffer", 0)
                        chunks = line[0:end].split(",")
                        # Extract K values and wSoS, save pair to list
                        K = int(chunks[0][7:])
                        wSoS = float(chunks[5])
                        wSoSList += [[K, wSoS]]
    return wSoSList

# plot K on x axis and wSoS on y axis to visualize "elbow" in decrease of wSoS vs K to find optimal K
def plotElbow(df, outputDir):
    fig, ax = plt.subplots(figsize = (6,8))
    ax.plot(df.K, df.wSoS, marker = 'o')
    ax.set_title('Within Cluster Sum of Squares Elbow:\nOptimal Number of Clusters K')
    ax.set_ylabel('Within Cluster Sum of Squares Error')
    ax.set_xlabel('Number of Clusters K')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True)) # integer only on x axis
    # write to output directory
    fig.savefig(os.path.join(outputDir, "wSoSPlot.pdf"), bbox_inches='tight')



def main():
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]

    # Flag if this is the output from multik or parallelk
    multik = False
    if len(sys.argv) == 4:
        multik = True

    # create output directory if doesn't already exist
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    wSoSList = []

    if not multik:
        # read within sum of squares error that was outputted for each K by KMeans program
        # returns list of lists of format [[k, wSoS], [k, wSoS], ...]
        wSoSList = readInput(inputDir)
    else:
        wSoSList = readMultiKInput(inputDir)

    # convert into dataframe with two columns for plotting
    df = pd.DataFrame(wSoSList, columns = ['K', 'wSoS'])
    df = df.sort_values('K')
    print(df)

    # plot K vs wSoS to visualize elbow and save to output directory
    plotElbow(df, outputDir)

main()