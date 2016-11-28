#pragma once

#include <deque>
#include <mr_task_factory.h>
#include <grpc++/grpc++.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using masterworker::TaskRequest;
using masterworker::TaskReply;
using masterworker::AssignTask;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);
		~Worker();

		/* DON'T change this function's signature */
		bool run();

		enum WorkerStatus { IDLE, RUNNING };
        WorkerStatus wrk_status;

        WorkerStatus get_status(){
            return Worker::wrk_status;
        }
        void set_status(WorkerStatus stat){
            Worker::wrk_status = stat;
        }
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		enum JobType { ALIVE = 1, MAP = 2, REDUCE = 3};
		AssignTask::AsyncService task_service;
		std::unique_ptr<ServerCompletionQueue> task_cq;
		ServerContext task_ctx;
		TaskRequest task_req;
		TaskReply task_reply;
		ServerAsyncResponseWriter<TaskReply> task_responder;
		std::unique_ptr<Server> task_server;
		class CallData {
			public:
				// Take in the "service" instance (in this case representing an asynchronous
				// server) and the completion queue "cq" used for asynchronous communication
				// with the gRPC runtime.
				CallData(AssignTask::AsyncService* service, ServerCompletionQueue* cq, int *q_id_, Worker* wrkr_)
					: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), q_id(q_id_),wrkr(wrkr_) {
						// Invoke the serving logic right away.
						Proceed();
					}

                // Proceed now returns the type of job it did
                JobType Proceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        service_->RequestDoTask(&ctx_, &request_, &responder_, cq_, cq_,
                                (void*)q_id);
								std::cout << "Requested task" << std::endl;
                    } else if (status_ == PROCESS) {

                        // The actual processing.
                        // Switch on request enum for actions
                        std::cout << "Processing" << std::endl;
                        switch(request_.job()){
                            case (TaskRequest::ALIVE):
                                {
                                    //do heartbeat stuff
                                    auto stat = wrkr->get_status();
                                    ret_val = ALIVE;
                                    break;
                                }

                            case (TaskRequest::MAP):
                                {
                                    std::cout << "MAPPING"<< std::endl;
                                    wrkr->set_status(RUNNING);
                                    auto mapper = get_mapper_from_task_factory("cs6210");
                                    mapper->map("some_input_map");
                                    ret_val = MAP;
                                    break;
                                }

                            case (TaskRequest::REDUCE):
                                {
                                    std::cout << "REDUCING"<< std::endl;
                                    wrkr->set_status(RUNNING);
                                    auto reducer = get_reducer_from_task_factory("cs6210");
                                    reducer->reduce("some_input_key_reduce", std::vector<std::string>({"some_input_vals_reduce"}));
                                    ret_val = REDUCE;
                                    break;
                                }
                            default:
                                {
                                    //shouldn't happen
                                    std::cout << "Got unkown job type"<< std::endl;
                                    break;
                                }
                        }//switch

                        //reply_.set_message(prefix + request_.name());

                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                        status_ = FINISH;
                        responder_.Finish(reply_, Status::OK, (void*)q_id);
                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        // Re-Queue this object for further processing
                        service_->RequestDoTask(&ctx_, &request_, &responder_, cq_, cq_,(void*) q_id);
                        return ret_val;
                    }
                }//Proceed

			private:
				// The means of communication with the gRPC runtime for an asynchronous
				// server.
				AssignTask::AsyncService* service_;
				// The producer-consumer queue where for asynchronous server notifications.
				ServerCompletionQueue* cq_;
				// Context for the rpc, allowing to tweak aspects of it such as the use
				// of compression, authentication, as well as to send metadata back to the
				// client.
				ServerContext ctx_;

				// What we get from the client.
				TaskRequest request_;
				// What we send back to the client.
				TaskReply reply_;
                int *q_id;
                JobType ret_val;
                Worker* wrkr;

				// The means to get back to the client.
				ServerAsyncResponseWriter<TaskReply> responder_;

				// Let's implement a tiny state machine with the following states.
				enum CallStatus { CREATE, PROCESS, FINISH };
				CallStatus status_;  // The current serving state.
		};// Call Data
        std::deque<CallData> mini_workers;


}; // Worker Class

Worker::~Worker(){
    task_server->Shutdown();
    task_cq->Shutdown();
}


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port): task_responder(&task_ctx), wrk_status(IDLE) {
    ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    builder.RegisterService(&task_service);
    task_cq = builder.AddCompletionQueue();
    task_server = builder.BuildAndStart();
    std::cout << "Worker listening on " << ip_addr_port << std::endl;

    // We only need 2 functions out of these workers. map/reduce and heartbeat
    // since the grpc example code is stateless, we needed to keep track that we are mapping / reducing and still alive
	int id = 0;
    mini_workers.emplace_back(&task_service, task_cq.get(),&id,this);
    //mini_workers.emplace_back(&task_service, task_cq.get(),1,this);

}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 4 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */

    void* tag;
    bool ok;
    while(true) {
        GPR_ASSERT(task_cq->Next(&tag,&ok));
        GPR_ASSERT(ok);
        // handle the request, de-ref the tag, as the index to the deque
        wrk_status = RUNNING;
		std::cout << "Tag: " << tag << std::endl << "&tag" << &tag << std::endl;
		//int index = static_cast<int>(reinterpret_cast<intptr_t>(tag));
        switch(mini_workers[*((int*)tag)].Proceed()){
            case(ALIVE):
                {
                    // we just did a heartbeat check
                    continue;
                    break;
                }
            case(REDUCE):
            case(MAP):
                {
                    // we just finished a map/reduce, set status to idle then return
                    wrk_status = IDLE;
                    return true;
                }
        }//switch
    }
}
