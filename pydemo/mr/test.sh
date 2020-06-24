set -x
rm *.json
timeout -k 2s 180s  python3.8 master.py &
sleep 1

timeout -k 2s 180s  python3.8 worker.py &
timeout -k 2s 180s  python3.8 worker.py &
timeout -k 2s 180s  python3.8 worker.py &
wait 

sort mr-out* | grep . > mr-wc-all
# rm mr-o*
# rm *.json