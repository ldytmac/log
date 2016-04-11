#include "LogMgr.h"

using namespace std;

int LogMgr::getLastLSN(int txnum) {
    if(tx_table.find(txnum) == tx_table.end()) {
	return NULL_LSN;
    }
    return tx_table[txnum].lastLSN;
}


void LogMgr::setLastLSN(int tx_num, int lsn) {
    tx_table[txnum].lastLSN = lsn;     
}


void LogMgr::flushLogTail(int maxLSN) {
    string toDisk;	// Information to flush to disk
    auto itr = logtail.begin();
    while(itr != logtail.end()) {
	if(*itr->getLSN() <= maxLSN) {
	    toDisk += *itr->toString();
	    logtail.erase(logtail.begin() + itr);
	} else ++itr;
    }
    // flush to disk
    se->updateLog(toDisk);
}


void LogMgr::analyze(vector <LogRecord*> log) {
    // Scan the log to find the most recent begin_checkpoint log record
    unsigned int start = 0;
    unsigned int end = 0;
    for(unsigned int i = 0 ; i < log.size(); ++i) {
	if(log[i]->getType == BEGIN_CKPT) {	// begin_checkpoint
	    start = i;
	}
	else if(log[i]->getType == END_CKPT) {	// end_checkpoint
	    end = i;
	    assert(end > start);
	    break;
	}
    }
    // Initialize dirty page table and transaction table
    ChkptLogRecord* end_checkpoint = dynamic_cast<ChkptLogRecord*>(log[end]);
    tx_table = end_checkpoint->getTxTable();
    dirty_page_table = end_checkpoint->getDirtyPageTable();
    // Scan the log and modify both tables, start from checkpoint
    for(unsigned int i = start; i < log.size(); ++i) {
	// Remove no longer active entry
	if(log[i]->getType == END) {
	    int tid = log[i]->getTxID();
	    if(tx_table.find(tid) != tx_table.end()) {
		tx_table.erase(tid);
	    }
	}
	// Encounter other types, update transaction table and DPT 
	else {
	    // Update transaction table
	    LogRecord* record = log[i];
	    int tid = record->getTxID();
	    TxStatus status = (record->getType() == COMMIT) ? C : U;
	    if(tx_table.find(tid) == tx_table.end()) {	// Newly encountered transaction
		txTableEntry(record->getLSN(), status)
		tx_table[tid] = txTableEntry;
	    }
	    else {
		tx_table[tid].lastLSN = record->getLSN();
		tx_table[tid].status = status;
	    }
	    // Update dirty page table
	    if(status == U) {
		UpdateLogRecord* updateRecord = dynamic_cast<UpdateLogRecord*>(record);
		int pid = updateRecord->getPageID();
		if(dirty_page_table.find(pid) == dirty_page_table.end()) {   // Newly encountered potential dirty page
		    dirty_page_table[pid] = record->getLSN();
		}
		else {
		    assert(record->getLSN() > dirty_page_table[pid]);
		}
	    }
    	}
    }
}


// Helper function to find the index of specified LSN in a log
unsigned int findLogIdx(const vector <LogRecord*> log, int start, int end, int specifiedLSN) {
    // Binary search
    unsigned int mid = start + (end - start)/2;
    if(log[mid]->getLSN() == specifiedLSN) return mid;
    if(log[mid]->getLSN() > specifiedLSN) end = mid -1;
    else start = mid + 1;
    return findLogIdx(log, start, end, specifiedLSN); 
}


// Helper function to check if a action needs to be redone 
bool needRedo(int pid, int lsn) {
    if(dirty_page_table.find(pid) == dirty_page_table.end()) return false;
    if(dirty_page_table[pid] > lsn) return false;
    if(se->getLSN(pid) >= lsn) return false;
    return true;
}


bool LogMgr::redo(vector <LogRecord*> log) {
    if(dirty_page_table.size() == 0) return true;
    int smallest_recLSN = *dirty_page_table.begin().second;
    // Find the record with smallest_recLSN
    unsigned int start = findLogIdx(log, 0, log.size()-1, smallest_recLSN);
    // Scan the log from start point 
    for(unsigned int i = start; i < log.size(); ++i) {
	// Examine redoable actions 
	LogRecord* record = log[i];	
	if(record->getType == UPDATE || record->getType == CLR) {
	    int pid;
	    int lsn = record->getLSN();
	    UpdateLogRecord* updateRecord = dynamic_cast<UpdateLogRecord*>(log[i]);  
	    CompensationLogRecord* compRecord = dynamic_cast<CompensationLogRecord*>(log[i]);
	    if(updateRecord) pid = updateRecord->getPageID;
	    else pid = compRecord->getPageID;

	    // Call helper function to check if this action needs to be redone
	    if(needRedo(pid, lsn)) {
		// Reapply logged action
		// Set the pageLSN on the page to the LSN of the redone log record
		int offset;
		string text;
		if(updateRecord) {
		    offset = updateRecord->getOffset();
		    text = updateRecord->getAfterImage();
		    if(!se->pageWrite(pid, offset, text, lsn)) return false;
		}
		else if(compRecord)
		    offset = compRecord->getOffset();
		    text = compRecord->getAfterImage();	
		    if(!se->pageWrite(pid, offset, text, lsn)) return false;
		}	
	    }
	}
    }
    // At the end of the redo phase, end type records are written for all transactions with status C
    auto itr = tx_table.begin();
    while(itr != tx_table.end()) {
	if(*itr->status == C) {
	    int lsn = se.nextLSN();
	    int prevLSN = *itr->second.lastLSN;
	    int txID = *itr->first;
	    LogRecord* endRecord = new LogRecord(lsn, prevLSN, txID, END);
	    logtail.push_back(endRecord);
	    tx_table.erase(tx_table.begin() + itr);
	} else ++itr;
    }
    return true;
}


void LogMgr::undo(vector <LogRecord*> log, int txnum = NULL_TX) {
    set<int> toUndo;
    // If it is general undo
    if(txnum == NULL_TX) {
	if(tx_table.size() ==0) return;	// Nothing to undo
	// Insert lastLSN values for all loser transactions into toUndo set
	for(auto itr = tx_table.begin(); itr != tx_table.end(); ++itr) {
	    toUndo.insert(*itr->lastLSN);
	}
    } 
    // If it is an abort
    else {
	int LastLSN = getLastLSN(int txnum);
	unsigned int index =  findLogIdx(log, 0, log.size()-1, lsn);
    	LogRecord* toUndoRecord = log[index];
	assert(toUndoRecord->getType() == UPDATE || toUndoRecord->getType() == CLR);
	toUndo.insert(LastLSN);

    }
    // Start from the max one until toUndo is empty
    while(!toUndo.empty()) {
    	int lsn = toUndo.rbegin();
    	unsigned int index =  findLogIdx(log, 0, log.size()-1, lsn);
    	LogRecord* toUndoRecord = log[index];
    	if(toUndoRecord->getType() == CLR) {
	    CompensationLogRecord* compRecord = dynamic_cast<CompensationLogRecord*>(toUndoRecord);
	    int undoNextLSN = compRecord->getUndoNextLSN();
	    // If this CLR has valid NextLSN to undo, insert this value to set
	    if(undoNextLSN != NULL_LSN) {
	    	toUndo.insert(undoNextLSN);
	    }
	    // Otherwise, write a new end record
	    else {
	    	int lsn = se.nextLSN();
	    	int prevLSN = toUndoRecord->getLSN();
	    	int txID = toUndoRecord-> getTxID();
	    	LogRecord* endRecord = new LogRecord(lsn, prevLSN, txID, END);
	    	logtail.push_back(endRecord);
	    	// Erase at here? Discard CLR?
	    	tx_table.erase(txID);
	    }	
	}		    
        else if(toUndoRecord->getType() == UPDATE) {
	    UpdateLogRecord* updateRecord = dynamic_cast<UpdateLogRecord*>(toUndoRecord); 
	    // Write a CLR
	    int lsn = se.nextLSN();
	    int prevLSN = toUndoRecord->getLSN();
	    int txID = toUndoRecord->getTxID();
	    int pageID = updateRecord->getPageID();
	    int pageOffset = updateRecord->getPageOffset();
	    string afterImage = updateRecord->getAfterImage();
	    string beforeImage = updateRecord->getBeforeImage();
	    int undoNextLSN = toUndoRecord->getprevLSN();
	    CompensationLogRecord* newCompRecord = new CompensationLogRecord(lsn, prevLSN, txID, pageID,
							           pageOffset, afterImage, undoNextLSN);
            logtail.push_back(newCompRecord);	
	    // Add to set
	    toUndo.insert(undoNextLSN);
	    // Undo the operation
	    se->pageWrite(pageID, pageOffset, beforeImage, prevLSN);
        }
        toUndo.erase(lsn);
    }
}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
    vector<LogRecord*> result; 
    stringstream ss(logstring);
    string recordString;
    while(getline(ss, recordString)) {
	LogRecord* record = se->stringToRecordPtr(record);
	assert(record);
	result.push_back(record);
    }
    return result;
}


void LogMgr::abort(int txid) {
    string wholeLog = se->getLog();
    vector <LogRecord*> log = stringToLRVector(wholeLog);
    
    int lsn = se.nextLSN();
    int prev_lsn = getLastLSN(txid);
    LogRecord* abortRecord = new LogRecord(lsn, prev_lsn, txid, ABORT);
    logtail.push_back(begin_check);  
  
    setLastLSN(txid, lsn);
    undo(log, txid);
}


void LogMgr::checkpoint() {
    int beginlsn = se.nextLSN();
    LogRecord* begin_check = new LogRecord(beginlsn, NULL_LSN, NULL_TX, BEGIN_CKPT);
    logtail.push_back(begin_check);	 

    int endlsn = se.nextLSN();
    LogRecord* begin_check = new ChkptLogRecord(endlsn, NULL_LSN, NULL_TX, tx_table, dirty_page_table);
    logtail.push_back(begin_check);

    if(!se->store_master(beginlsn)) {
	cerr << "store master fails!" << endl;
	return;
    }

    // Flush logtail to disk?
}


void LogMgr::commit(int txid) {
    int lsn = se.nextLSN();
    int prev_lsn = getLastLSN(txid);

    LogRecord* commitRecord = new LogRecord(lsn, prev_lsn, txid, COMMIT);
    logtail.push_back(commitRecord);  

    setLastLSN(txid, lsn); 
    tx_table[txid].status = C;

    prev_lsn = lsn;
    lsn = se.nextLSN();
    prev_lsn = getLastLSN(txid);
    LogRecord* endRecord = new LogRecord(lsn, prev_lsn, txid, END);
    logtail.push_back(endRecord);  
    setLastLSN(txid, lsn);  

    tx_table.erase(txid);

    // Flush log tail?
}


void LogMgr::pageFlushed(int page_id) {
    pageLSN = se->getLSN(page_id);
    flushLogTail(pageLSN);
    dirty_page_table.erase(page_id);
}


void LogMgr::recover(string log) {
    vector <LogRecord*> recoverLog = stringToLRVector(log);
    analyze(recoverLog);
    if(redo(recoverLog)) undo(recoverLog);
}


int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
    int lsn = se.nextLSN();
    int prev_lsn = getLastLSN(txid);

    UpdateLogRecord* updateRecord = new UpdateLogRecord(lsn, prev_lsn, tx_id, page_id, offset, oldtext, input);
    logtail.push_back(updateRecord);  

    setLastLSN(txid, lsn); 
    tx_table[txid].status = U;
    dirty_page_table[page_id] = lsn;

    return lsn;
}


void setStorageEngine(StorageEngine* engine) {
    assert(engine);
    se = engine;
}


