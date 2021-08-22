import sys

filenamestring = sys.argv[1]

f1 = open(filenamestring,"r")
f2 = open(filenamestring+".clean","w")

linesAlreadySeen = set()
lineiterobj = iter(f1)

firstErrInExec = True

execCount = 0

f2.write("~~~~~~~~~~~~exec:"+str(execCount)+"~~~~~~~~~~~~\n")
f2.write("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n\n\n")

while True:
    try:
        line = next(lineiterobj)
        #this is the first error in this execution
        if firstErrInExec:
        #what type of err are we looking at?
            if "ERROR ~~~~~~~~~~~~" in line:
                #go to the line after >> End Range Action:
                while ">> End Range Action:" not in line:
                    line = next(lineiterobj)
                #get the important location
                location = next(lineiterobj)
                #if the next line is a location, continue. Else, get the one after that
                if ".cc" in line or ".c" in line or ".h" in line:
                    continue
                else:
                    location = next(lineiterobj)
                #if we haven't seen it, note it and write it
                if location not in linesAlreadySeen:
                    f2.write("~~~~~~~~~~~~begin in "+str(execCount)+"~~~~~~~~~~~~\n")
                    f2.write(location)
                    f2.write("~~~~~~~~~~~~\n\n")
                    linesAlreadySeen.add(location)
                print(execCount,"had bug")
                firstErrInExec = False
            if "IN Setting EndRange ~~~~~~~~~~~~" in line:
                #first get read causing the write
                #
                #go to the line after >> The write causing the bug:
                while ">> The write causing the bug:" not in line:
                    line = next(lineiterobj)
                #get the important location: if we haven't seen it, note it and write it
                location = next(lineiterobj)
                #if the next line is a location, continue. Else, get the one after that
                if ".cc" in line or ".c" in line or ".h" in line:
                    continue
                else:
                    location = next(lineiterobj)
                if location not in linesAlreadySeen:
                    f2.write("~~~~~~~~~~~~end in "+str(execCount)+"~~~~~~~~~~~~\n")
                    f2.write(location)
                    f2.write("~~~~~~~~~~~~\n\n")
                    linesAlreadySeen.add(location)
                firstErrInExec = False
                print(execCount,"had bug")
            if "PMVerifier found Robustness" in line:
                #get the location of the read
                next(lineiterobj)
                readLoc = next(lineiterobj)
                #get the location of the write it is reading from
                next(lineiterobj)
                next(lineiterobj)
                writeLoc = next(lineiterobj)
                #go to the line after >> Possible fix: Insert flushes after write(s):
                for i in range(7):
                    line = next(lineiterobj)
                #get the important locations in a tuple
                locationList = [readLoc,writeLoc]
                #assert(len(locationList)==2), "init len is: "+str(len(locationList))
                while "****************************" not in line:
                    if ".cc" in line or ".c" in line or ".h" in line:
                        locationList.append(line)
                    line = next(lineiterobj)
                locationTuple = tuple(locationList)
                #assert(len(locationList)>=4), "len is: "+str(len(locationList))
                #if we haven't seen it, note it and write it
                if locationTuple not in linesAlreadySeen:
                    f2.write("~~~~~~~~~~~~robustness in "+str(execCount)+"~~~~~~~~~~~~\n")
                    for lineToWrite in locationTuple:
                        f2.write(lineToWrite)
                    f2.write("~~~~~~~~~~~~\n\n")
                    linesAlreadySeen.add(locationTuple)
                firstErrInExec = False
                print(execCount,"had robustness bug")
            if "Crash Execution" in line:
                firstErrInExec = True
                execCount+=0.5
        #this is not the first error in this execution: continue until we get to the next
        else:
            if "Crash Execution" in line:
                firstErrInExec = True
                execCount+=0.5
    except StopIteration:
        break
