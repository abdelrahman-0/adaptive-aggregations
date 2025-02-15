MAX_NODES="${MAX_NODES:-4}"

DATE_TIME=$(date +%Y_%m_%d_%H_%M_%S)
LOGS_DIR=$(dirname "$0")
EXP_DIR="$LOGS_DIR/$DATE_TIME"

mkdir $EXP_DIR

cd $EXP_DIR
for NODE_ID in $(seq 0 $(($MAX_NODES-1)));
do
  touch node_$NODE_ID.txt
done
touch notes.md

echo "created ${EXP_DIR}"
