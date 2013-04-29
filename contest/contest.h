#ifndef CONTEST_H
#define CONTEST_H

#include "dberror.h"

extern RC setUpContest (int numPages);
extern RC shutdownContest (void);

extern long getContestIOs (void);

#endif
