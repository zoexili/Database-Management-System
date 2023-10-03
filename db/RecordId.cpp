#include <db/RecordId.h>
#include <stdexcept>

using namespace db;

//
// RecordId
//

// TODO pa1.4: implement
RecordId::RecordId(const PageId *pid, int tupleno) {
    Pid = pid;
    TupleNo = tupleno;
}

bool RecordId::operator==(const RecordId &other) const {
    // TODO pa1.4: implement
    if (*Pid == *(other.getPageId()) && TupleNo == other.getTupleno()) {
        return true;
    }
    return false;
}

const PageId *RecordId::getPageId() const {
    // TODO pa1.4: implement
    return Pid;
}

int RecordId::getTupleno() const {
    // TODO pa1.4: implement
    return TupleNo;
}

//
// RecordId hash function
//

std::size_t std::hash<RecordId>::operator()(const RecordId &r) const {
    // TODO pa1.4: implement
    size_t pageHash = std::hash<PageId>()(*r.getPageId());
    size_t tpnoHash = std::hash<int>()(r.getTupleno());
    return pageHash ^ (tpnoHash << 1);
}
