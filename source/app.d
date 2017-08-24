


import network.node;
import zhang2018.common.Log;
import std.conv;

int main(string[] argv)
{
	if(argv.length < 5)
	{
		log_info("raftexample ID apiport cluster join");
		log_info("raftexample 1 2110 \"127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112\" false ");
		return -1;
	}
	ulong ID = to!ulong(argv[1]);
	load_log_conf("default.conf");
	node.instance.start(ID , argv[2] , argv[3], to!bool(argv[4]));
	node.instance.wait();
	return 0;
}




