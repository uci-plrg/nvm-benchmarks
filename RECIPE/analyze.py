import re
import argparse
import sys


class AutoTunerArgParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Parsing the output log of the CSolver auto tuner ...')
        self.parser.add_argument('--file', '-f', metavar='out.log', type=str, nargs=1,help='output log of running the autotuner ...')
        self.args = self.parser.parse_args()
        
    def getFileName(self):
        return self.args.file[0]


def main():
	global configs
	argprocess = AutoTunerArgParser()
	execNum = ''
	crashPoint = ''
	naiveNum = ''
	result = []
	with open(argprocess.getFileName()) as file:
		for line in file:
			if line.startswith("Total executions"):
				execNum = line;
			elif line.startswith("Total number of stop points"):
				crashPoint = line
			elif line.startswith("Total naive execution"):
                                naiveNum=line

	print(execNum)
	print(crashPoint)
	print(naiveNum)

if __name__ == "__main__":
	main()
