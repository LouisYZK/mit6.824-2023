rm mr*
python3.8 seqmr.py
sort mr-seq | grep . > mr-wc-correct || exit 1
# cp ~/6.824/src/main/mr-correct-wc.txt ./mr-correct-wc.txt
rm *.json

START=`date +%s`;

timeout -k 2s 180s  python3.8 master.py & 
sleep 1

timeout -k 2s 180s  python3.8 worker.py & 
timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py &
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py &
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py & 
# timeout -k 2s 180s  python3.8 worker.py &
# timeout -k 2s 180s  python3.8 worker.py & 

wait 
END=`date +%s`;


sort mr-out* | grep . > mr-wc-all
rm mr-o*
rm *.json

echo '***' Starting test.
if cmp mr-wc-all mr-wc-correct
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi
take=$(( END - START ))
echo Time taken to execute commands is ${take} seconds.