for i in {1..10}; do
    OK=$(echo ruok | nc 127.0.0.1 $1)
    if [ "$OK" == "imok" ]; then
      exit 0
    fi
    sleep 1
done

exit 1
