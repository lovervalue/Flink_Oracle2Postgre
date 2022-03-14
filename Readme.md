这是一个flink流项目    
从oracle实时流程到postgres  
需要注意的地方是flink的内存，编码需要设置  
./bin/flink run  -c org.Job_Data_SXAC01 /app/flink-1.14.3/examples/Flink_O2P.jar              
./bin/flink stop -m 127.0.0.1:8081 4297e978fd24e7e908500a2f48cabe33