#include <iostream>
#include <string.h>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
using namespace std;
const int N = 99;
const int UNIT_NUM = 5000;
const int GEM_MAX=50;
const double P=0.8;
const double B=0.1;
double dis[N] [N] ;

struct pos{
int no;
int x,y;
};
struct map{
pos M[100];
};
map MM;
struct solve{
int path[N] ;
double length;

bool operator < ( const struct solve&other) const //用于群体的排序
{
return length < other.length;
}
}S[UNIT_NUM];

solve Best = { {0} ,1000000};

void init(){
    ifstream in("source");

    for(int i=0;i<N;i++){
        in>>(MM.M[i] .no)>>(MM.M[i] .x)>>(MM.M[i] .y);
    }

    for(int i=0;i<N;i++){
        for(int j=0;j<N;j++){
        dis[i] [j] =sqrt( (MM.M[i] .x - MM.M[j] .x) * (MM.M[i] .x - MM.M[j] .x) + (MM.M[i] .y - MM.M[j] .y) * (MM.M[i] .y - MM.M[j] .y) );
        }
    }
}

double getDis( solve s){
    double sum=0;

    for(int i=0;i<N-1;i++){
        sum+=dis[ s.path[i] ][ s.path[i+1] ];
    }

    return sum;
}

void init_group( solve a[]){
    for(int i=0;i<UNIT_NUM;i++){
        for( int j=0;j<N;j++){
            a[i] .path[j] =j;
        }
    }

    for(int i=0;i<UNIT_NUM;i++){
        random_shuffle(a[i] .path,a[i] .path+N);
        a[i] .length=getDis(a[i] );
    }
}

int getPos(solve a,int c){
    for(int i=0;i<N;i++){
        if(a.path[i] ==c){
            return i;
        }
    }
    return -1;
}

void swap(int *a,int *b){
    int tmp=*a;
    *a=*b;
    *b=tmp;
}

void reverse(solve &a,int s,int e){
    int s1=s,s2=e;
    while(s1<s2){
        swap( a.path[s1] , a.path[s2] );
        s1++;s2--;
    }
}

void rotate(solve &a,int s,int e,int p){
    reverse(a,p,e-1);
    reverse(a,s,p-1);
    reverse(a,s,e-1);
}

void cross_group(solve &a,solve &b){

    solve res;
    double len1 = dis[a.path[0] ][ a.path[1] ];
    double len2 = dis[b.path[0] ][ b.path[1] ];

    if (len1 <= len2){
    res.path[0] = a.path[0];
    }else
    {
    res.path[0] = b.path[0];
    }

    int pos=res.path[0];

    int p=getPos(a,pos);
    int q=getPos(b,pos);
    rotate(a,0,N,p);
    rotate(b,0,N,q);
    int ch;
    for(int i=1;i<N;i++){
        double dis1=dis[ a.path[i-1] ][ a.path[i] ];
        double dis2=dis[ b.path[i-1]] [ b.path[i] ];
        if( dis1<dis2 ) {
            ch=a.path[i] ;
            res.path[i] =ch;
            q=getPos(b,ch);
            rotate(b,i,N,q);
        }else{
            ch=b.path[i] ;
            res.path[i] =ch;
            p=getPos(a,ch);
            rotate(a,i,N,p);
    }
}

    res.length=getDis(res);

    memcpy( &a, &res, sizeof(solve));

    if(res.length<Best.length){
        memcpy(&Best, &res, sizeof(solve));
    }
}

void varation_group(solve a[]){
    double temp;

    int num = UNIT_NUM * B;
    while (num--)
    {
        int k = rand() % UNIT_NUM;

        int i = rand() % N;
        int j = rand() % N;

        temp = a[k] .path[i] ;
        a[k] .path[i] = a[k] .path[j] ;
        a[k] .path[j] = temp;

        a[k] .length=getDis(a[k] );
    }

}


void evolution_group(solve a[]){
    int g = GEM_MAX;

    int num1 = UNIT_NUM * ( 1 - P);
    int num2 = UNIT_NUM * P;

    while(g--){
        sort(a, a + UNIT_NUM);
        if (a[0].length < Best.length){
            memcpy(&Best, &a[0], sizeof(solve));
        }

        for(int i=0;i<num1;i++){
            memcpy(&a[num2+i],&a[i] ,sizeof(solve));
        }//选择保留遗传给下一代

        //交叉
        for (int i = 0; i < UNIT_NUM / 2; i++)
        {
            cross_group(a[i] , a[ UNIT_NUM - i -1]);
        }

        //变异
        varation_group(a);
    }

    if (a[0].length < Best.length){
        memcpy(&Best, &a[0], sizeof(solve));
    }
}

int main(){
srand(time(NULL));
init();

init_group( S );

evolution_group(S);
for(int i=0;i<N;i++){
cout<<"->"<<Best.path[i] ;
}cout<<endl;
cout<<Best.length<<endl;
}
