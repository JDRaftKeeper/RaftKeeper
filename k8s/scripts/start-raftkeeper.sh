(cd $RAFTKEEPER_DIR/bin/ && python3 config_generator.py "$@")
if [ $? != 0 ]; then exit $?; fi

cat $RAFTKEEPER_DIR/conf/config.xml

echo -e "\n"

(cd $RAFTKEEPER_DIR && ./lib/raftkeeper server --config=conf/config.xml)

# prevent pod from completing
tail -f /dev/null
