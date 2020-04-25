#ifndef PIKA_CMDSTATS_H_
#define PIKA_CMDSTATS_H_

#include <stdint.h>
#include <map>
#include <atomic>

#include "strings.h"
#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
typedef slash::RWLock RWLock;

const int32_t TPFirstGrade = 5;				//5ms - 200ms
const int32_t TPFirstGradeSize = 40;
const int32_t TPSecondGrade = 25;		    //225ms - 700ms
const int32_t TPSecondGradeSize = 20;
const int32_t TPThirdGrade = 250;			    //950ms - 3200ms
const int32_t TPThirdGradeSize = 10;

const int32_t TPNum = TPFirstGradeSize + TPSecondGradeSize + TPThirdGradeSize;
const int32_t IntervalNum = 5;
const int32_t DelayKindNum = 8;
void TpDelayInit();
void LastRefreshTimeInit();

class DelayInfo  {
public:
	DelayInfo(int32_t interval = 0): interval_(interval),
	calls_(0),
	usecs_(0),
	qps_(0),
	usecsmax_(0),
	avg_(0),
	tp90_(0),
	tp99_(0),
	tp999_(0),
	tp9999_(0),
	tp100_(0),
	delay50ms_(0),
	delay100ms_(0),
	delay200ms_(0),
	delay300ms_(0),
	delay500ms_(0),
	delay1s_(0),
	delay2s_(0),
	delay3s_(0){
		InitTp();
		InitDelayCount();
	}

	uint32_t interval_;
	//int64_t last_refresh_time_;
	std::atomic<uint64_t> calls_;
	std::atomic<uint64_t> usecs_;
	//std::atomic<uint64_t> usecsmax_;
	std::atomic<uint64_t> qps_;
	uint64_t usecsmax_;
	uint64_t avg_;

	std::atomic<int64_t> tp_[TPNum];
	int64_t tp90_;
	int64_t tp99_;
	int64_t tp999_;
	int64_t tp9999_;
	int64_t tp100_;

	std::atomic<int64_t> delay_count_[DelayKindNum];
	int64_t delay50ms_;    
	int64_t delay100ms_;
	int64_t delay200ms_;
	int64_t delay300ms_;
	int64_t delay500ms_;
	int64_t delay1s_;
	int64_t delay2s_;
	int64_t delay3s_;

	void InitTp();
	void InitDelayCount();
	int64_t GetTP(float persents);
	void Refresh4TP(std::string& cmd);
	void RefreshTpInfo(std::string& cmd);
	void ResetTpInfo();

	void RefreshDelayInfo();
	void ResetDelayInfo();
};



class OpStats {
public:
	OpStats(std::string cmd): opstr_(cmd),
	 total_calls_(0),
	 total_usecs_(0),
	 errors_(0){
	 	InitTpInfo();
	}
	void IncrOpStats(int64_t responseTime, bool err);
	//void ResetStats();
	void IncrTP(int64_t duration);
	void InitTpInfo();
	void IncrDelayNum(int64_t duration);
	void RefreshOpStats(int32_t index);
	std::string GetOpStatsByInterval(int32_t interval);

private:
	std::string opstr_;		
	std::atomic<uint64_t> total_calls_;
	std::atomic<uint64_t> total_usecs_;

	DelayInfo delay_info_[IntervalNum];
	
	std::atomic<uint64_t> errors_;
};

// for timeout info
class CmdStats  {
public:
	CmdStats():
	total_(0),
	errors_(0),
	qps_(0),
	last_total_(0){
		pthread_rwlock_init(&rwlock_, NULL);
		opstats_["ALL"] = new OpStats("ALL");
		TpDelayInit();
		LastRefreshTimeInit();
	}
	~CmdStats() { 
		{
			//退出的时候销毁new的对象
			RWLock l(&rwlock_, true);
			std::map<std::string, OpStats*>::iterator iter = opstats_.begin(); 
			while (iter != opstats_.end()) {
				if (iter->second != NULL) {
					delete iter->second;
					iter->second = NULL;
				}
				iter++;
			}
		}
		pthread_rwlock_destroy(&rwlock_); 
	}

	OpStats* GetOpStatsByCmd(std::string cmd);
	//新建命令对应的OpStats
	OpStats* CreateOpStats(std::string& cmd);

	std::string GetOpStats(int32_t index);
	std::string GetCmdStatsByInterval(int32_t interval);
	
	void ResetStats();
	void IncrOpStatsByCmd(std::string cmd, int64_t responseTime, bool err);
	void RefreshCmdStats();


private:
	int64_t total_; 
	int64_t errors_;
	int64_t qps_;

	int64_t last_total_;
	//uint64_t last_refresh_time_;
	std::map<std::string, OpStats*> opstats_;

	pthread_rwlock_t rwlock_;
};


#endif
