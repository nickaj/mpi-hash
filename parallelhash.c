/* Copyright (c) 2015 The University of Edinburgh. */

/* Licensed under the Apache License, Version 2.0 (the "License"); */
/* you may not use this file except in compliance with the License. */
/* You may obtain a copy of the License at */

/*     http://www.apache.org/licenses/LICENSE-2.0 */

/* Unless required by applicable law or agreed to in writing, software */
/* distributed under the License is distributed on an "AS IS" BASIS, */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and */
/* limitations under the License. */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

typedef unsigned long int numb;
/* Should be 64 bit wide, to hold the square of: */
/* If you change this, also change "atol" in main */
#define modulus 1073741827
#define multipl 33554467

typedef numb * obj;

numb N;     /* Number of objects to hash */
size_t m;   /* Size of objects in bytes, rounded up to be a multiple of
               sizeof(numb) */
numb k;     /* Number of times each object is expected */

int count;
int remote=0;
int local=0;
int status = 0;
MPI_Comm comm;
MPI_Info info;
MPI_Win win,win2;
//int rank,size,status;
int rank,size;

int *testArray;
int testVar;


/* Some fast way to create funny data: */

numb val = 1234567;

numb next(void)
{
  val = (val * multipl) % modulus;
  count++;
  return val;
}


void resetvalue(void)
{

  int i,j,nval;
  numb dummy;

  val = 1234567;
  nval = N / size;
  for (j=0;j<rank*nval;j++){
    for(i=m/sizeof(numb); i> 0;i--){
      dummy = next();

    }
  }

}


obj newobj(void)
{
  obj o,o2;
  int i;
  o = malloc(m);
  o2 = o;
  for (i = m/sizeof(numb); i > 0;i--) *o2++ = next();
  return o;
}

/* Code for hashing: */

/*obj  *hashtab;*/
numb *hashtab;
numb *hashcount;
numb hashlen;
numb collisions;
numb nobj;



void inithash(numb N)
{

  MPI_Init(NULL,NULL);
  status=MPI_Info_create(&info);
  status=MPI_Info_set(info,"same_size","true");

  comm=MPI_COMM_WORLD;
  MPI_Comm_rank(comm,&rank);
  MPI_Comm_size(comm,&size);

  hashlen = 2*N+1;
  nobj = 2*N/size + 1;
  /*    hashtab = calloc(hashlen,sizeof(obj));*/
  hashtab = (numb *)calloc(nobj,sizeof(numb));
  hashcount = (numb *)calloc(nobj,sizeof(numb));
  if (hashtab == NULL || hashcount == NULL) exit(1);
  collisions = 0;
  count=0;


  /*    testArray = (int *) malloc(sizeof(int)*10);*/
  status = MPI_Win_create(hashtab,nobj*sizeof(int),sizeof(numb),MPI_INFO_NULL,comm,&win);
  status = MPI_Win_create(hashcount,nobj*sizeof(int),sizeof(numb),MPI_INFO_NULL,comm,&win2);

}

numb f(obj o)
/* Our hash function, this should do: */
{

  numb x = 0;
  int i;
  for (i = m/sizeof(numb); i > 0; i--){
    x += *o++;
  }
  return x % hashlen;
}

numb hashlookup(obj o)
/* Never fill up the hash table! Returns the number of times a value
 * was seen so far. If an obj is seen for the first time, it is stored
 * without copying it, do not free the object in this case, ownership
 * goes over to the hash table. */
{
  numb v;
  int destRank, destPos;
  numb localHash,localCount;

  v = f(o);
  localHash=0;
  localCount=0;

  while (1) {
    /* work out who and where the hash should go */
    destRank = v/nobj;
    destPos = v - nobj*destRank;
    /* Get the value in the hash table */
    if(destRank!=rank){
      remote++;
    }
    else{
      local++;
    }
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE,destRank,0,win);
    status = MPI_Get(&localHash,1,MPI_INT,destRank,destPos,1,MPI_INT,win);
    MPI_Win_unlock(destRank,win);
    if (localHash) {
      /*          if (memcmp(o,hashtab[v],m)) {*/
      if(!(*o==localHash)){
        v++;
        destPos++;
        if (v >= hashlen) v = 0;
        if (destPos >= nobj){
          /* don't fall off the end, go to the next one */
          destPos=0;
          destRank++;
          /* and for rank too */
          if(destRank>=size) destRank=0;

        }
        collisions++;
      } else {   /* Found! */
        /* get the count */
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE,destRank,0,win2);
        MPI_Get(&localCount,1,MPI_INT,destRank,destPos,1,MPI_INT,win2);
        localCount++;
        MPI_Put(&localCount,1,MPI_INT,destRank,destPos,1,MPI_INT,win2);
        MPI_Win_unlock(destRank,win2);
        /*        hashcount[v]++;
                  return hashcount[v];*/
        return localCount;
      }
    } else {   /* Definitely not found */
      /*        hashtab[yv] = o;*/
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE,destRank,0,win);
      /* What an I copying here */
      MPI_Put(o,1,MPI_INT,destRank,destPos,1,MPI_INT,win);
      MPI_Win_unlock(destRank,win);

      /*        hashcount[v] = 1;*/
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE,destRank,0,win2);
      localCount=1;
      MPI_Put(&localCount,1,MPI_INT,destRank,destPos,1,MPI_INT,win2);
      MPI_Win_unlock(destRank,win2);

      return localCount;
    }

  }
}

int main(int argc, char *argv[])
{
  numb i,j,c;
  obj o;

  int repeatObj,sumRO,sumCollisions;
  repeatObj=sumRO=sumCollisions=0;

  if (argc != 4) {
    puts("Usage: hashexample N m k");
    puts("       where N is the number of objects to hash");
    puts("         and m is the length of the objects in bytes");
    puts("         and k is the number of times objects are expected");
    return 0;
  }
  N = atol(argv[1]);
  m = (size_t) atol(argv[2]);
  m = (m + sizeof(numb) - 1) / sizeof(numb);
  m *= sizeof(numb);
  k = atol(argv[3]);


  inithash(N);

  if(rank==0){
    /* printf("%s:initialise hash N=%d N/MPI_TASKS=%d\n",argv[0],N,N/size); */
    printf("%s:initialise hash N=%d N/MPI_TASKS=%d\n",argv[0],(int)N,(int)(N/size));
  }

  /*    for(i=0;i<nobj;i++){
        hashtab[i]=rank*nobj+i;
        }


        MPI_Win_fence(0,win);
        c=rank*100;
        status=-1;
        printf("rank[%d] testVar %d\n",rank,c);
        status=MPI_Get(&c,1,MPI_INT,1,3,1,MPI_INT,win);
        MPI_Win_fence(0,win);
        printf("Get? rank[%d] testVar %d status %d\n",rank,c,status);
  */



  for (i = 1; i <= k; i++) {
    resetvalue();
    for (j = 0; j < N/size; j++) {
      o = newobj();
      c = hashlookup(o);
      if (c > 1)
        free(o);
      if (c != i) {
        repeatObj++;
      }
    }
  }

  /* write out stuff */
  /*    for(j=0;j<nobj;j++){
        if(hashtab[j]){
        printf("rank[%d] j %d #j %d *j %d collisions %d\n",rank, ((rank*nobj)+j),hashtab[j], hashtab[j],collisions);
        }
        }*/

  /* need to aggregate collisions and repititions */
  MPI_Reduce(&collisions,&sumCollisions,1,MPI_INT,MPI_SUM,0,comm);
  MPI_Reduce(&repeatObj,&sumRO,1,MPI_INT,MPI_SUM,0,comm);
  if(rank==0){
    printf("finn: collisions %d repetitions %d\n",sumCollisions,sumRO);
  }


  status = MPI_Win_free(&win);
  status = MPI_Win_free(&win2);
  MPI_Finalize();

  return 0;
}
