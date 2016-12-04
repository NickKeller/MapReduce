#pragma once

#include <thread>
#include <chrono>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <mr_task_factory.h>
#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using grpc::Channel;
using masterworker::PingRequest;
using masterworker::MapRequest;
using masterworker::ReduceRequest;
using masterworker::TaskReply;
using masterworker::ShardPiece;
using masterworker::AssignTask;

enum WorkerStatus {MAP, REDUCE, IDLE, DEAD, ALIVE};

//some kind of map of shards to worker processes
struct workerInfo{
	/*
	MAP - Currently running map task
	REDUCE - Currently running reduce task
	IDLE - Currenty waiting for a job
	DEAD - Worker is down
	*/
public:
	//the ip address of the worker
	std::string ip_addr;
	//the specific shard that it's working on
	FileShard shard;
	//keeps the state of the worker
	WorkerStatus state;
	//keeps track of the amount of heartbeats that it has been running for
	int heartbeats;
	//keeps track of the job id that this worker is working on
	std::string job_id;
	//the name of the output file that this is supposed to write to
	std::string output_file;
};

struct AsyncWorkerCall{
	//ClientContext
	ClientContext context;
	//response reader
	std::unique_ptr<ClientAsyncResponseReader<TaskReply>> response_reader;
	//worker that it points to
	workerInfo* cur_worker;
	//The status used for the reply
	Status rpc_status;
	//the TaskReply that this worker is going to receive information from
	TaskReply reply;
};

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();


	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		//store the spec
		MapReduceSpec spec;
		std::vector<FileShard> shards;
		std::vector<workerInfo*> workers;
		std::vector<std::string> output_files;
		//keeps track of the jobs that have been completed
		std::vector<std::string> jobs;
		//the list of files that map processes output
		std::vector<std::string> map_output_files;
		//the list of files that reduce processes output
		std::vector<std::string> reduce_output_files;
		//the CompletionQueue used for map and reduce events
		CompletionQueue master_cq;
		//the max number of heartbeats to wait until reassigning a task
		int heartbeat_limit;

		//private functions
		WorkerStatus pingWorkerProcess(const workerInfo* worker);
		void assignMapTask(workerInfo* worker);
		void assignReduceTask(workerInfo* worker, int section);
		bool allWorkersDead(void);
		bool newJob(const std::string& job_id);
		TaskReply getSingleResponse(const std::string& task_type);


};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : spec(mr_spec), shards(file_shards) {
	//populate the workers vector
	for(int i = 0; i < mr_spec.worker_ipaddr_ports.size(); i++){
		std::string wrker = mr_spec.worker_ipaddr_ports.at(i);
		std::cout << "Worker: " << wrker << std::endl;
		//have to assign a shard using a constructor like this. It will get reassigned later
		workerInfo* worker = new workerInfo;
		worker->ip_addr = wrker;
		worker->state = IDLE;
		worker->heartbeats = 0;
		workers.push_back(worker);
		std::cout << "Worker with ipAddr " << worker->ip_addr << " added" << std::endl;
	}
	//set to 30 seconds
	heartbeat_limit = 5*30;

	//populate the output files vector
	for (size_t i = 0; i < spec.num_output_files; i++) {
		std::string fileName = spec.output_dir + "/output_" + std::to_string(i);
		output_files.push_back(fileName);
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	bool map_complete = false;
	int last_shard_assigned = 0;
	//200ms heartbeat seems fair
	int heartbeat = 200;
	int num_shards_mapped = 0;
	int num_map_tasks_to_do = shards.size();
	while(!map_complete){
		//ping all worker processes
		//this can be done synchronously
		for(int i = 0; i < workers.size(); i++){
			workerInfo* worker = workers.at(i);
			WorkerStatus state = pingWorkerProcess(worker);
			//state is either going to be ALIVE or DEAD
			if(state == ALIVE){
				worker->heartbeats++;
				std::cout << "Worker at " << worker->ip_addr << " is ALIVE for " << worker->heartbeats << " heartbeats" << std::endl;
				//reassign shard if the worker has been working for more than 1 minute
				if(worker->heartbeats > (heartbeat_limit)){
					FileShard stalled_shard = worker->shard;
					std::cout << "-------------------------Setting shard " << stalled_shard.shard_id << " to be redone" << std::endl;
					shards.push_back(stalled_shard);
				}
			}
			else{
				std::cout << "Worker at " << worker->ip_addr << " is DEAD" << std::endl;
				//pop it's task, if the process was doing something
				if(worker->state == MAP){
					//it was busy doing something
					FileShard dead_shard = worker->shard;
					shards.push_back(dead_shard);
				}
				worker->state = DEAD;
			}
		}

		if(allWorkersDead()){
			std::cout << "ERROR: ALL WORKER PROCESSES ARE DEAD" << std::endl;
			return false;
		}

		//assign shards to alive worker processes, only if there are shards left to assign
		for(int i = 0; i < workers.size(); i++){
			workerInfo* worker = workers.at(i);
			if(worker->state == IDLE && shards.size() > 0){
				//assign the next shard
				FileShard shard_to_assign = shards.back();
				shards.pop_back();
				worker->shard = shard_to_assign;
				worker->state = MAP;
				std::cout << "Assigned shard: " << worker->shard.shard_id << " to worker process: " << worker->ip_addr << std::endl;
				//fire off a map task
				assignMapTask(worker);
			}
			else if(shards.size() == 0){
				std::cout << "All shards assigned" << std::endl;
			}
		}

		//block on a single response
		TaskReply reply = getSingleResponse("MAP");
		//make sure Successful
		if(reply.task_type().compare("MAP") == 0){
			//Successful
			if(newJob(reply.job_id())){
				//add to the list of output files
				std::cout << "New Job, adding jod id: " << reply.job_id() << " to list of completed jobs" << std::endl;
				map_output_files.push_back(reply.out_file());
				num_shards_mapped++;
			}
		}


		//if all shards have been mapped, set the bool to complete
		if(num_shards_mapped == num_map_tasks_to_do){
			map_complete = true;
		}
		//wait for finish, reassign processes as needed
		std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat));
	}//end while

	//clear the job ids and the output files
	jobs.clear();

	//spin up reduce processes
	bool reduce_complete = false;
	//fire up as many reduce tasks as nr_output_files
	int num_reduces_done = 0;
	int last_reduce_assigned = 0;
	int num_reduce_tasks_to_do = output_files.size();
	while(!reduce_complete){
		//ping all worker processes
		//this can be done synchronously
		for(int i = 0; i < workers.size(); i++){
			//need to grab the address, otherwise we're not modifying the
			//actual workerInfo struct. Yay C++ semantics
			workerInfo* worker = workers.at(i);
			WorkerStatus state = pingWorkerProcess(worker);
			//state is either going to be ALIVE or DEAD
			if(state == ALIVE){
				worker->heartbeats++;
				std::cout << "Worker at " << worker->ip_addr << " is ALIVE for " << worker->heartbeats << " heartbeats" << std::endl;
			}
			else{
				std::cout << "Worker at " << worker->ip_addr << " is DEAD" << std::endl;
				//was this running a reduce task? If so, reassign the output file it was working on
				if(worker->state = REDUCE){
					std::string dead_file = worker->output_file;
					output_files.push_back(dead_file);
				}
				worker->state = DEAD;
			}

		}

		if(allWorkersDead()){
			std::cout << "ERROR: ALL WORKER PROCESSES ARE DEAD" << std::endl;
			return false;
		}
		//assign shards to alive worker processes, only if there are shards
		//left to assign
		for(int i = 0; i < workers.size(); i++){
			workerInfo* worker = workers.at(i);
			if(worker->state == IDLE && output_files.size() > 0){
				//assign the next reduce task
				std::string output_file_to_assign = output_files.back();
				output_files.pop_back();
				worker->output_file = output_file_to_assign;
				worker->state = REDUCE;
				std::cout << "Assigned reduce: " << worker->output_file << " to worker process: " << worker->ip_addr << std::endl;
				last_reduce_assigned++;
				//fire off a map task
				assignReduceTask(worker, i);
			}
			else if(output_files.size() == 0){
				std::cout << "All reduce tasks assigned" << std::endl;
			}
		}

		//block on a single response
		TaskReply reply = getSingleResponse("REDUCE");
		//make sure Successful
		if(reply.task_type().compare("REDUCE") == 0){
			//Successful
			if(newJob(reply.job_id())){
				//add to the list of output files
				std::cout << "New Job, adding jod id: " << reply.job_id() << " to list of completed jobs" << std::endl;
				reduce_output_files.push_back(reply.out_file());
				num_reduces_done++;
			}
		}

		//if all shards have been mapped, set the bool to complete
		if(num_reduces_done == num_reduce_tasks_to_do){
			reduce_complete = true;
		}
		//wait for finish, reassign processes as needed
		std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat));
	}
	//time to output the final files
	std::cout << "Output files:" << std::endl;
	for(auto file : reduce_output_files){
		std::cout << "\t-" << file << std::endl;
	}
	std::cout << "Master run" << std::endl;
	return true;
}

WorkerStatus Master::pingWorkerProcess(const workerInfo* worker){
	std::string ip_addr = worker->ip_addr;
	WorkerStatus res = ALIVE;
	PingRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(ip_addr,grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	Status status;
	std::cout << "Pinging worker at " << ip_addr << std::endl;
	//sync here, because I need the state of all the workers before I can assign map tasks
	status = stub->Ping(&context,request,&reply);

	//the workers aren't going to be doing anything if they're alive, so we only need to check if the
	//request failed
	if(!status.ok()){
		res = DEAD;
		std::cout << "WARNING: Worker " << ip_addr << " is not available" << std::endl;
	}

	return res;
}

void Master::assignMapTask(workerInfo* worker){
	MapRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker->ip_addr, grpc::InsecureChannelCredentials()));
	request.set_user_id(spec.user_id);
	//special thing to do for shards
	FileShard shard = worker->shard;
	for (int i = 0; i < shard.pieces.size(); i++) {
		file_key_offset* pair = shard.pieces.at(i);
		ShardPiece* piece = request.add_shard();
		piece->set_file_name(pair->filename);
		piece->set_start_index(pair->startOffset);
		piece->set_end_index(pair->endOffset);
	}
	std::string jobID = "map_" + std::to_string(worker->shard.shard_id);
	request.set_job_id(jobID);
	request.set_num_reducers(spec.num_output_files);
	worker->job_id = jobID;
	std::cout << "Assigning map task to " << worker->ip_addr << std::endl;
	//sync
	//Status status;
	// status = stub->Map(&context,request,&(worker->reply));
	//async
	AsyncWorkerCall* call = new AsyncWorkerCall;
	call->cur_worker = worker;
	call->response_reader = stub->AsyncMap(&call->context, request, &master_cq);
	//also call finish
	std::cout << "Calling finish for job " << worker->job_id << std::endl;
	call->response_reader->Finish(&call->reply, &call->rpc_status,(void*)call);

	std::cout << "Returning from assignMapTask" << std::endl;
}

void Master::assignReduceTask(workerInfo* worker, int section ){
	ReduceRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker->ip_addr, grpc::InsecureChannelCredentials()));
	ClientContext context;
	request.set_user_id(spec.user_id);
	request.set_output_file(worker->output_file);
	request.set_job_id(worker->output_file);
	request.set_section(std::to_string(section));
	for(auto input_file : map_output_files){
		request.add_input_files(input_file);
	}
	worker->job_id = worker->output_file;
	std::cout << "Assigning reduce task to " << worker->ip_addr << std::endl;
	//sync
	//Status status;
	// status = stub->Reduce(&context,request,&(worker->reply));
	//async
	AsyncWorkerCall* call = new AsyncWorkerCall;
	call->cur_worker = worker;
	call->response_reader = stub->AsyncReduce(&call->context, request, &master_cq);
	//also call finish
	std::cout << "Calling finish for job " << worker->job_id << std::endl;
	call->response_reader->Finish(&call->reply, &call->rpc_status,(void*)call);

	std::cout << "Returning from assignReduceTask" << std::endl;
}

bool Master::allWorkersDead(){
	return false;
	for (size_t i = 0; i < workers.size(); i++) {
		workerInfo* worker = workers.at(i);
		if(worker->state != DEAD){
			return false;
		}
	}
	return true;
}

bool Master::newJob(const std::string& job_id){
	for(auto job : jobs){
		if(job.compare(job_id) == 0){
			//not a new job
			return false;
		}
	}
	//this is a new job, add it to the list of completed jobs, then return true
	jobs.push_back(job_id);
	return true;
}

TaskReply Master::getSingleResponse(const std::string& task_type){
	//default is failed, will get overwritten if successful
	TaskReply reply;
	reply.set_task_type("FAIL");
	void* got_tag;
	bool ok = false;
	std::cout << "Calling next" << std::endl;
	master_cq.Next(&got_tag, &ok);
	//now, find the correct worker to use
	std::cout << "Getting worker" << std::endl;
	AsyncWorkerCall* call = static_cast<AsyncWorkerCall*>(got_tag);
	workerInfo* worker = call->cur_worker;
	//w = findWorker((int*)got_tag);
	std::cout << "Found worker: " << worker->ip_addr << std::endl;
	GPR_ASSERT(ok);

	if(call->rpc_status.ok()){
		std::cout << "Successful " << task_type << " for " << worker->ip_addr << std::endl;
		worker->state = IDLE;
		worker->heartbeats = 0;
		//increment the number of shards mapped only if this was a job that hasn't already been completed
		reply = call->reply;
	}
	else{
		std::cout << "---------------ERROR IN " << task_type << " FOR " << worker->ip_addr << std::endl;
		std::cout << call->rpc_status.error_code() << std::endl;
		std::cout << call->rpc_status.error_message() << std::endl;
	}

	return reply;
}
