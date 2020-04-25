#include <glog/logging.h>

#include "pika_cmdstats.h"
#include "slash/include/slash_string.h"

// 单位: s
int32_t IntervalMark[IntervalNum] = {1, 10, 60, 600, 3600};
uint64_t LastRefreshTime[IntervalNum] = {0};
// 单位: ms
int32_t DelayNumMark[DelayKindNum] = {50, 100, 200, 300, 500, 1000, 2000, 3000};
// 参照数组，辅助统计tp信息
int64_t TpDelay[TPNum] = {0};

void TpDelayInit() {
	for (int32_t i = 0; i < TPNum; i++) {
		if (i < TPFirstGradeSize) {
			TpDelay[i] = (i + 1) * TPFirstGrade;
		} else if (i < TPFirstGradeSize + TPSecondGradeSize) {
			TpDelay[i] = TPFirstGradeSize * TPFirstGrade + (i - TPFirstGradeSize + 1) * TPSecondGrade;
		} else {
			TpDelay[i] = TPFirstGradeSize * TPFirstGrade + TPSecondGradeSize * TPSecondGrade  + (i - TPFirstGradeSize -  TPSecondGradeSize + 1) * TPThirdGrade;
		}
	}
}

void LastRefreshTimeInit() {
	uint64_t now = slash::NowMicros();
	for (int32_t i = 0; i < IntervalNum; i++) {
		LastRefreshTime[i] = now;
	}
}

void DelayInfo::InitTp(){
	for (int32_t i = 0; i < TPNum; i++) {
		tp_[i] = 0;
	}
}

void DelayInfo::InitDelayCount(){
	for (int32_t i = 0; i < DelayKindNum; i++) {
		delay_count_[i] = 0;
	}
}

int64_t DelayInfo::GetTP(float persents) {
	if (calls_ == 0 || persents <= 0 || persents > 1) {
		return 0;
	}

	int64_t tpnum = int64_t( float(calls_) * persents );
	int64_t count = 0;
	int32_t i;

	for (i = 0; i < TPNum; i++) {
		count += tp_[i];
		if (count >= tpnum || i == (TPNum-1)) {
			break;
		}
	}

	if (i >= 0 && i < TPNum) {
		return TpDelay[i];
	}

	return -1;
}

//persents support 0 < persents <= 1 only
void DelayInfo::Refresh4TP(std::string& cmd){
	if (calls_ == 0) {
		tp90_ = 0;
		tp99_ = 0;
		tp999_ = 0;
		tp9999_ = 0;
		return;
	}

	int64_t tpnum1 = int64_t( float(calls_) * 0.9 );
	int64_t tpnum2 = int64_t( float(calls_) * 0.99 );
	int64_t tpnum3 = int64_t( float(calls_) * 0.999 );
	int64_t tpnum4 = int64_t( float(calls_) * 0.9999 );

	int32_t index1 = -1, index2 = -1, index3 = -1, index4 = -1;
	int64_t count = 0;
	int32_t i;

	for (i = 0; i < TPNum; i++) {
		count += tp_[i];
		if (count >= tpnum1 || i == TPNum-1) {
			index1 = i;
			break;
		}
	}

	if (count >= tpnum2 || i == TPNum-1) {
		index2 = i;
	} else {
		for (i = i+1; i < TPNum; i++) {
			count += tp_[i];
			if (count >= tpnum2 || i == TPNum-1) {
				index2 = i;
				break;
			}
		}
	}

	if (count >= tpnum3 || i == TPNum-1) {
		index3 = i;
	} else {
		for (i = i+1; i < TPNum; i++) {
			count += tp_[i];
			if (count >= tpnum3 || i == TPNum-1) {
				index3 = i;
				break;
			}
		}
	}

	if (count >= tpnum4 || i == TPNum-1) {
		index4 = i;
	} else {
		for (i = i+1; i < TPNum; i++) {
			count += tp_[i];
			if (count >= tpnum4 || i == TPNum-1) {
				index4 = i;
				break;
			}
		}
	}

	if (i == TPNum-1 && tp_[i] == 0) {
		LOG(WARNING) << "fillTpInfo err: cmd-[" << cmd << "] tpinfo is unavailable";
	}

	if (index1 >= 0 && index2 >= index1 && index3 >= index2 && index4 >= index3 && index4 < TPNum) {
		tp90_ = TpDelay[index1];
		tp99_ = TpDelay[index2];
		tp999_ = TpDelay[index3];
		tp9999_ = TpDelay[index4];
		return;
	}

	LOG(WARNING) << "fillTpInfo err: cmd-[" << cmd << "] reset exception tpinfo";
	tp90_ = -1;
	tp99_ = -1;
	tp999_ = -1;
	tp9999_ = -1;
}

void DelayInfo::RefreshTpInfo(std::string& cmd){
	Refresh4TP(cmd);
	tp100_ = usecsmax_ / 1e3;
	if (calls_ == 0) {
		avg_ = 0;
	} else {
		avg_ = usecs_ / 1e3 / calls_;
	}
}

void DelayInfo::ResetTpInfo(){
	//LOG(WARNING) << "ResetTpInfo: call_ :" << calls_;
	calls_ = 0;
	usecs_ = 0;
	usecsmax_ = 0;
	for (int32_t i = 0; i < TPNum; i++) {
		tp_[i] = 0;
	}
}

void DelayInfo::RefreshDelayInfo() {
	delay50ms_ = delay_count_[0];
	delay100ms_ = delay_count_[1];
	delay200ms_ = delay_count_[2];
	delay300ms_ = delay_count_[3];
	delay500ms_ = delay_count_[4];
	delay1s_ = delay_count_[5];
	delay2s_ = delay_count_[6];
	delay3s_ = delay_count_[7];
}

void DelayInfo::ResetDelayInfo() {
	for (int32_t i = 0; i < DelayKindNum; i++) {
		delay_count_[i] = 0;
	}
}


void OpStats::InitTpInfo(){
	for(int32_t i = 0; i < IntervalNum; i++ ){
		delay_info_[i].interval_ = IntervalMark[i];
	}
}

// response_time单位为us
void OpStats::IncrOpStats(int64_t response_time, bool err){
	total_calls_++;
	total_usecs_ += response_time;
	if (err) {
		errors_++;
	}
	
	//统计tp数据
	IncrTP(response_time);
	//统计超时命令数量
	IncrDelayNum(response_time/1e3);
}

//duration单位为us
void OpStats::IncrTP(int64_t duration){
	int64_t duration_ms = duration / 1e3;
	int32_t index = -1;
	if (duration_ms <= 0) {
		index = 0;
	} else if (duration_ms <= TPFirstGrade*TPFirstGradeSize) {
		index = (duration_ms + TPFirstGrade - 1) / TPFirstGrade - 1;
	} else if (duration_ms <= TPFirstGrade*TPFirstGradeSize + TPSecondGrade*TPSecondGradeSize) {
		index = (duration_ms - TPFirstGrade*TPFirstGradeSize + TPSecondGrade - 1) / TPSecondGrade + TPFirstGradeSize - 1;
	} else if (duration_ms <= TPFirstGrade*TPFirstGradeSize + TPSecondGrade*TPSecondGradeSize + TPThirdGrade*TPThirdGradeSize) {
		index = (duration_ms - TPFirstGrade*TPFirstGradeSize - TPSecondGrade*TPSecondGradeSize + TPThirdGrade - 1) / TPThirdGrade + TPFirstGradeSize + TPSecondGradeSize - 1;
	} else {
		index = TPNum - 1;
	}

	if (index < 0) {
		return;
	}

	for (int32_t i = 0; i < IntervalNum; i++) {
		delay_info_[i].calls_++;
		delay_info_[i].usecs_ += duration;
		// 此处不加锁存在会被其他线程修改
		int64_t usecsmax = delay_info_[i].usecsmax_;
		// 允许50ms的误差范围，避免频繁设置usecsmax_
		if (duration > usecsmax + 5000) {
			for(; ;){
				bool ok = __sync_bool_compare_and_swap(&(delay_info_[i].usecsmax_), usecsmax, duration);
				if (ok) {
					break;
				} else {
					usecsmax = delay_info_[i].usecsmax_;
					if (duration <= usecsmax + 5000) {
						//LOG(WARNING) << "CompareAndSwap return false and break, newMax is [" << duration << "] lastMax is [" << usecsmax << "]";
						break;
					}
					LOG(WARNING) << "CompareAndSwap return false and try again, newMax is [" << duration << " us] lastMax is [" << usecsmax << " us]";
				}
			}
		}

		delay_info_[i].tp_[index]++;
	}
}

void OpStats::IncrDelayNum(int64_t duration){
	int64_t index = -1;
	for (int32_t i = 0; i < DelayKindNum; i++) {
		if (duration >= DelayNumMark[i]) {
			index = i;
		} else {
			break;
		}
	}
	//没有超过50ms的命令
	if (index == -1) {
		return;
	}

	for (int32_t i = 0; i < IntervalNum; i++) {
		for(int32_t j = 0; j <= index; j++) {
			delay_info_[i].delay_count_[j]++;
		}
	}
}

void OpStats::RefreshOpStats(int32_t index){
	if (index < 0 || index >= IntervalNum) {
		return;
	}
	//此处存在time_cost为0的情况
	int64_t time_cost = (slash::NowMicros() - LastRefreshTime[index]) / 1e6;
	if (time_cost <= 0) {
		return;
	}
	float normalized = (float)(delay_info_[index].calls_) / (float)(time_cost);
	/*if (delay_info_[index].calls_ == 0) {
		LOG(WARNING) << "delay_info_[" << index << "].calls_ == 0" ;
	}*/
	delay_info_[index].qps_ = int64_t(normalized + 0.5);

	delay_info_[index].RefreshTpInfo(opstr_);
	delay_info_[index].ResetTpInfo();

	delay_info_[index].RefreshDelayInfo();
	delay_info_[index].ResetDelayInfo();
	//delay_info_[index].last_refresh_time_ = slash::NowMicros() / 1e6;
}

std::string OpStats::GetOpStatsByInterval(int32_t index){
	if (index < 0) {
		return "";
	}

	std::string json = "";
	json += "\t  {\r\n";
	json += "\t\t\"opstr\": \"" + opstr_ + "\",\r\n";
	json += "\t\t\"interval\": " + std::to_string(delay_info_[index].interval_) + ",\r\n";
	json += "\t\t\"total_calls\": " + std::to_string(total_calls_) + ",\r\n";
	json += "\t\t\"total_usecs\": " + std::to_string(total_usecs_) + ",\r\n";


	uint64_t calls = delay_info_[index].calls_;
	uint64_t usecs = delay_info_[index].usecs_;
	uint64_t usecs_percall = 0;
	if (calls == 0 || usecs == 0) {
		usecs_percall = 0;
	} else {
		usecs_percall = calls / usecs;
	}

	json += "\t\t\"calls\": " + std::to_string(calls) + ",\r\n";
	json += "\t\t\"usecs\": " + std::to_string(usecs) + ",\r\n";
	json += "\t\t\"usecs_percall\": " + std::to_string(usecs_percall) + ",\r\n";
	json += "\t\t\"errors\": " + std::to_string(errors_) + ",\r\n";
	json += "\t\t\"qps\": " + std::to_string(delay_info_[index].qps_) + ",\r\n";

	json += "\t\t\"avg\": " + std::to_string(delay_info_[index].avg_) + ",\r\n";
	json += "\t\t\"tp90\": " + std::to_string(delay_info_[index].tp90_) + ",\r\n";
	json += "\t\t\"tp99\": " + std::to_string(delay_info_[index].tp99_) + ",\r\n";
	json += "\t\t\"tp999\": " + std::to_string(delay_info_[index].tp999_) + ",\r\n";
	json += "\t\t\"tp9999\": " + std::to_string(delay_info_[index].tp9999_) + ",\r\n";
	json += "\t\t\"tp100\": " + std::to_string(delay_info_[index].tp100_) + ",\r\n";

	json += "\t\t\"delay50ms\": " + std::to_string(delay_info_[index].delay50ms_) + ",\r\n";
	json += "\t\t\"delay100ms\": " + std::to_string(delay_info_[index].delay100ms_) + ",\r\n";
	json += "\t\t\"delay200ms\": " + std::to_string(delay_info_[index].delay200ms_) + ",\r\n";
	json += "\t\t\"delay300ms\": " + std::to_string(delay_info_[index].delay300ms_) + ",\r\n";
	json += "\t\t\"delay500ms\": " + std::to_string(delay_info_[index].delay500ms_) + ",\r\n";
	json += "\t\t\"delay1s\": " + std::to_string(delay_info_[index].delay1s_) + ",\r\n";
	json += "\t\t\"delay2s\": " + std::to_string(delay_info_[index].delay2s_) + ",\r\n";
	json += "\t\t\"delay3s\": " + std::to_string(delay_info_[index].delay3s_) + "\r\n";;

	json += "\t  }";
	return json;
}


OpStats* CmdStats::GetOpStatsByCmd(std::string cmd){
	slash::StringToUpper(cmd);

	{
		RWLock l(&rwlock_, false);
		std::map<std::string, OpStats*>::iterator it = opstats_.find(cmd);
		if(it != opstats_.end()) {
			return it->second;
		}
	}
	//如果命令对应的OpStats不存在则新建
	return CreateOpStats(cmd);
}

OpStats* CmdStats::CreateOpStats(std::string& cmd){
	slash::StringToUpper(cmd);

	RWLock l(&rwlock_, true);
	std::map<std::string, OpStats*>::iterator iter = opstats_.find(cmd);

	if(iter == opstats_.end()) {
		opstats_[cmd] = new OpStats(cmd);
	}

	return opstats_[cmd];
}

void CmdStats::ResetStats(){ //ResetCmdStats
	;
}

void CmdStats::IncrOpStatsByCmd(std::string cmd, int64_t response_time, bool err){
	slash::StringToUpper(cmd);
	OpStats* opstat = NULL;

	if (err) {
		errors_++;
	}
	total_++;

	// 不能保证opstat指向的结构不会被其他线程释放
	opstat = GetOpStatsByCmd(cmd);
	if (opstat != NULL) {
		opstat->IncrOpStats(response_time, err);
	}
	
	opstat = GetOpStatsByCmd("ALL");
	if (opstat != NULL) {
		opstat->IncrOpStats(response_time, err);
	}
}

void CmdStats::RefreshCmdStats(){
	if ((slash::NowMicros() - LastRefreshTime[0]) / 1e6 < IntervalMark[0]) {
		// 不满足最小的刷新周期1s
		return;
	}

	uint64_t now = slash::NowMicros();
	float normalized = (float)(total_ - last_total_) / ((float)(now - LastRefreshTime[0]) / 1e6);
	qps_ = int64_t(normalized + 0.5);

	// 开始统计tp信息
	{
		RWLock l(&rwlock_, false);
	    for (int32_t i = 0; i < IntervalNum; i++) {
			if ( ((now - LastRefreshTime[i]) / 1e6) < IntervalMark[i] ) {
				continue;
			}
			std::map<std::string, OpStats*>::iterator iter = opstats_.begin(); 
		    while(iter != opstats_.end()){
		        iter->second->RefreshOpStats(i);
		        iter++;
		    }
		    LastRefreshTime[i] = slash::NowMicros();
		}
	}

	last_total_ = total_;
	//last_refresh_time_ = slash::NowMicros() / 1e6;
}

// 给infe delay命令使用
std::string CmdStats::GetCmdStatsByInterval(int32_t interval){
	int32_t index = -1;
	for (int32_t i = 0; i < IntervalNum; i++) {
		if (IntervalMark[i] == interval) {
			index = i;
			break;
		}
	}
	if (interval != 0 && index < 0) {
		return "interval not in [1, 10, 60, 600, 3600]";
	}

	std::string json = "";

	json += "{\r\n";

	json += "\t\"total\": " + std::to_string(total_) + ",\r\n";
	json += "\t\"errors\": " + std::to_string(errors_) + ",\r\n";
	json += "\t\"qps\": " + std::to_string(qps_) + ",\r\n";
	json += "\t\"cmd\": [\r\n";
	if (interval == 0) {
		for (int32_t i = 0; i<IntervalNum; i++) {
			json += GetOpStats(i) + "\r\n";
		}
	} else {
		json += GetOpStats(index);
	}
	json += "\r\n\t]\r\n";


	json += "}\r\n";
	return json;
}

std::string CmdStats::GetOpStats(int32_t index){
	std::string json = "";

	RWLock l(&rwlock_, false);
	//统计ALL命令，放到最开始
	json += opstats_["ALL"]->GetOpStatsByInterval(index);
	
	//统计其他命令
	std::map<std::string, OpStats*>::iterator iter;
    for(iter = opstats_.begin(); iter != opstats_.end(); iter++){
    	if (iter->first == "ALL" || iter->second == NULL) {
    		continue;
    	}
        json += ",\r\n" + iter->second->GetOpStatsByInterval(index);
    }

	return json;
}

