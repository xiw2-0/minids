/// RPC server binds method name to member functions.
/// Uses a map to transform string to member function calls.
/// It uses while(1) loop to wait for 
/// In the member function call, it parses from the request and
/// and call the coresponding method in client protocol handler
/// or chunkserver handler.
/// After return from handlers, it serializes the return values to
/// socket. And return the rpc calls. 