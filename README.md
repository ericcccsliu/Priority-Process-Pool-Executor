This is a modified version of the ProcessPoolExecutor that allows for priority scheduling of tasks. When submitting tasks
through the submit() method, the priority can be specified as a keyword argument. The priority is used to order tasks
within the work queue, with lower priority numbers taking precedence. 

Developed for CX Strategies. 

