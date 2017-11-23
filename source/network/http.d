module network.http;
import zhang2018.dreactor.aio.AsyncTcpBase;
import zhang2018.dreactor.event.Poll;
import zhang2018.common.Log;
import network.node;
import std.string;
import std.conv;
import protocol.Msg;

enum MAX_HTTP_REQUEST_BUFF = 1024 * 16;



enum RequestMethod
{
	METHOD_GET = 0, 
	METHOD_SET = 1
};

struct RequestCommand
{
	RequestMethod Method;
	string		  Key;
	string		  Value;
	size_t		  Hash;
};




class http : AsyncTcpBase
{
	this(Poll poll ,   byte[] buffer)
	{
		super(poll);
		readBuff = buffer;
	}

	bool is_request_finish(ref bool finish, ref string url , ref string strbody)
	{
		import std.typecons : No;
		
		string str = cast(string)_buffer;
		long header_pos = indexOf(str , "\r\n\r\n");
		
		if( header_pos == -1)
		{
			finish = false;
			return true;
		}
		
		string strlength = "content-length: ";
		int intlength = 0;
		long pos = indexOf(str , strlength , 0 , No.caseSensitive);
		if( pos != -1)
		{
			long left = indexOf(str , "\r\n" , cast(size_t)pos);
			if(pos == -1)
				return false;
			
			strlength = cast(string)_buffer[cast(size_t)(pos + strlength.length) .. cast(size_t)left];
			intlength = to!int(strlength);
		}
		
		
		if(header_pos + 4 + intlength == _buffer.length)
		{
			finish = true;
		}
		else
		{
			finish = false;
			return true;
		}
		
		long pos_url = indexOf(str , "\r\n");
		if(pos_url == -1)
			return false;
		
		auto strs = split(cast(string)_buffer[0 .. cast(size_t)pos_url]);
		if(strs.length < 3)
			return false;
		
		url = strs[1];
		strbody = cast(string)_buffer[cast(size_t)(header_pos + 4) .. $];
		
		return true;
	}

	bool do_response(string strbody)
	{
		auto res = log_format("HTTP/1.1 200 OK\r\nServer: kiss\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s"
			, strbody.length , strbody);
		auto ret = doWrite(cast(byte[])res , null , 
			delegate void(Object obj){
				close();
			});
		if(ret == 1)
			return false;

		return true;
	}

	bool process_request(string url , string strbody)
	{
		string path;
		long pos = indexOf(url , "?");
		string[string] params;
		if(pos == -1){
			path = url;
		}
		else{
			path = url[0 .. pos];
			auto keyvalues = split(url[pos + 1 .. $] , "&");
			foreach( k ; keyvalues)
			{
				auto kv = split(k , "=");
				if(kv.length == 2)
					params[kv[0]] = kv[1];
			}
		}

		if(path == "/get")
		{
			auto key = "key" in params;
			if(key == null || key.length == 0)
				return do_response("params key must not empty");

			RequestCommand command = { Method:RequestMethod.METHOD_GET , Key: *key , Hash:this.toHash()};
			node.instance().Propose(command , this);
			return true;
		}
		else if(path == "/set")
		{

			auto key = "key" in params;
			auto value = "value" in params;
			if(key == null || value == null || key.length == 0)
				return do_response("params key  must not empty or value not exist");

			RequestCommand command = { Method:RequestMethod.METHOD_SET , Key: *key ,Value : *value , Hash:this.toHash()};
			node.instance().Propose(command , this);
			return true;
		}
		else if(path == "/add")
		{
			

			auto nodeID = "ID" in params;
			auto Context = "Context" in params;
			if(nodeID == null || nodeID.length == 0 || Context.length == 0 || Context == null)
				return do_response("ID or Context must not empty");

			ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeAddNode ,Context:*Context };
			node.instance().ProposeConfChange(cc);
			return do_response("have request this add conf");
			
		}
		else if(path == "/del")
		{
			auto nodeID = "ID" in params;
			if(nodeID == null || nodeID.length == 0)
				return do_response("ID must not empty");
			ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeRemoveNode };
			node.instance().ProposeConfChange(cc);
			return do_response("have request this remove conf");
		}
		else
		{
			return do_response("can not sovle " ~ path);
		}

	}


	override bool doRead(byte[] data , int length)
	{
		_buffer ~= data[0 .. length];
		bool finish;
		string strurl;
		string strbody;
		if(!is_request_finish(finish ,strurl,strbody ))
			return false;

		if(finish)
		{
			return process_request(strurl , strbody);
		}
		else if(_buffer.length >= MAX_HTTP_REQUEST_BUFF)
		{
			return false;
		}

		return true;
	}

	override  bool onClose() {

		node.instance().delPropose(this);
		return super.onClose();
	}


	private byte[] _buffer;
}

