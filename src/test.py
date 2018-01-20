import commonmd as cmd
import time

ex_time = time.time()
a = cmd.getOrderbook('ALL') 
print( time.time() - ex_time )
