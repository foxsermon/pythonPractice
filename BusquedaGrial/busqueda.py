import sys
import os
import re
import time

def uShallNotPass():
   try:
      #input("Appuyez sur Entree pour continuer")
      raw_input("Appuyez sur Entree pour continuer, CTRL+Z pour annuler")
      print "\n"
   except:
      pass

def main(argv):
   searchingStr = sys.argv[1]
   pathStr = os.getcwd()
   try:
      pathStr = sys.argv[2]
   except:
      print "No path defined. used current path. ", pathStr

   syms = ['\\', '|', '/', '-']
   bs = '\b'
   totalCtn = 0

   dictStats = {}

   for dirpath, dirs, files in os.walk(pathStr):
      for filename in files:
         fname = os.path.join(dirpath, filename)
         printInfo = True
         with open(fname) as fp:
            line = fp.readline()
            cnt = 1
            foundStr = False
            spin = 0
            shallPass = 1
            lineCtn = 1
            while line:
               c = line.find(searchingStr)
               if c > -1:
                  totalCtn += 1
                  if printInfo:
                     print "\033[1;37;40m File : ", fname, "\033[0;37;40m\n"
                     printInfo = False

                  print("\033[1;36m{} ->\033[1;m\033[0;37;40m  {}".format(cnt,  re.sub(searchingStr, "\\033[1;42m" + searchingStr + "\\033[1;m\\033[0;37;40m ", line.rstrip())))
                  lineCtn += 1

                  shallPass += 1
                  if shallPass == 25:
                     uShallNotPass()
                     shallPass = 1

                  foundStr = True
                  searchObj = re.search(r'(.*)ERROR(.*?)exception(.*?).*', line, re.M | re.I)
                  if searchObj:
                     lnCnt = 0
                     lnStop = 200
                     errBodyCtn = 1
                     while lnCnt < lnStop:
                        line = fp.readline()
                        if len(line) == 0:
                           break
                        print "\033[0;37;40m", re.sub(searchingStr, "\\033[1;42m" + searchingStr + "\\033[1;m\\033[0;37;40m ", line.rstrip())
                        lnCnt += 1
                        cnt += 1
                        excBody = line.find("*****************************")
                        if excBody > -1:
                           errBodyCtn += 1

                        if errBodyCtn > 4:
                           errBodyCtn = 1
                           break

               else:
                  sys.stdout.write("\b%s" % syms[spin])
                  sys.stdout.flush()
                  #time.sleep(.00002)
                  spin += 1
                  if spin > 3:
                     spin = 0

               line = fp.readline()
               cnt += 1
            if foundStr:
               dictStats[fname] = lineCtn - 1
               print "\n\033[0;37;40m================================================================================\n"
               uShallNotPass()

   print "\n\033[0;37;40m Found : ", totalCtn, " matches"
   if totalCtn > 0:
      for k, v in dictStats.items():
         print "{1} \t-> File {0}".format(k, v)

if __name__ == "__main__":
   main(sys.argv[1:])