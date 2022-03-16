#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <cstdlib>
#include <papi.h>
#include <fstream>

using namespace std;

#define SYSTEMTIME clock_t
/**
 * @brief Number os PAPI events
 * 
 */
#define NUMBER_EVENTS 2

/**
 * @brief Number of events added to EventSet
 * 
 */
#define STRING_MAX 256

#define BLOCK_START_SIZE 128

#define BLOCK_END_SIZE 512

#define BLOCK_MUL_MIN 4096

#define BLOCK_MUL_MAX 10240

#define BLOCK_MUL_INTERVAL 2048

#define MAX_PATH_SIZE 256


 /*                     PAPI INTERFACE                                               */

void handle_error (int retval)
{
  printf("PAPI error %d: %s\n", retval, PAPI_strerror(retval));
  exit(1);
}

void init_papi() {
  int retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT && retval < 0) {
    printf("PAPI library version mismatch!\n");
    exit(1);
  }
  if (retval < 0) handle_error(retval);

  std::cout << "PAPI Version Number: MAJOR: " << PAPI_VERSION_MAJOR(retval)
            << " MINOR: " << PAPI_VERSION_MINOR(retval)
            << " REVISION: " << PAPI_VERSION_REVISION(retval) << "\n";
}

/**
 * @brief Inicia a contagem de eventos pré definidos no argumento EventSet.
 * 
 * @param EventSet Conjunto de eventos a ser contabilizados
 */
void start_papi_counters(int& EventSet){

	// Start counting
	int ret = PAPI_start(EventSet);
	if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;
}

/**
 * @brief Para a contagem e regista os valores dos contadores PAPI no array values por ordem de inserção
 * 
 * @param EventSet Conjunto d eeventos pré definidos a serem contabilizados
 * @param values Array de valores a serem preenchidos
 */
void stop_papi_counters(int& EventSet, long long* values){

	int ret = PAPI_stop(EventSet, values);
	if (ret != PAPI_OK) cout << "ERROR: Stop PAPI" << endl;

	ret = PAPI_reset( EventSet );
	if ( ret != PAPI_OK )
		std::cout << "FAIL reset" << endl; 
}

//                        Operations


void OnMult(int& EventSet, int m_ar, int m_br, char* st) {
	long long values[NUMBER_EVENTS];
	SYSTEMTIME Time1, Time2;
	
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;

	
		
    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	start_papi_counters(EventSet);
    Time1 = clock();

	for(i=0; i<m_ar; i++)
	{	for( j=0; j<m_br; j++)
		{	temp = 0;
			for( k=0; k<m_ar; k++)
			{	
				temp += pha[i*m_ar+k] * phb[k*m_br+j];
			}
			phc[i*m_ar+j]=temp;
		}
	}


    Time2 = clock();
	stop_papi_counters(EventSet, values);
	double operation_time =  (double)(Time2 - Time1) / CLOCKS_PER_SEC;

	//size, time, L1 DCM, L2 DCM
	sprintf(st, "%d, %3.3f, %lld, %lld\n",m_ar, operation_time, values[0], values[1] );

    free(pha);
    free(phb);
    free(phc);
	
	
}

void OnMultRange(int& EventSet, int startSize, int endSize, int interval){
	int current_size = startSize;

	char* text;

	ofstream outfile;
   	outfile.open("./data/mul_600_3000.csv");


	outfile<<"Size, Time, L1_DCM, L2_DCM"<<endl;

	text = (char*)malloc(STRING_MAX * sizeof(char));

	while(current_size<=endSize){
		OnMult(EventSet, current_size, current_size, text);
		outfile<< text;
		cout<<text;
		current_size+= interval;
	}
	outfile.close();
	free(text);
}

// add code here for line x line matriz multiplication
void OnMultLine(int& EventSet,int m_ar, int m_br, char* st)
{
	long long values[NUMBER_EVENTS];
    SYSTEMTIME Time1, Time2;
	
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

		
    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	start_papi_counters(EventSet);
    Time1 = clock();

	for(i=0; i<m_ar; i++)
	{	
		for( k=0; k<m_ar; k++)
		{
			for( j=0; j<m_br; j++)
			{	
				phc[i*m_ar+j] += pha[i*m_ar+k] * phb[k*m_br+j];
			}
		}
	}


	stop_papi_counters(EventSet, values);
    Time2 = clock();


	double operation_time =  (double)(Time2 - Time1) / CLOCKS_PER_SEC;
	sprintf(st, "%d, %3.3f, %lld, %lld\n",m_ar, operation_time, values[0], values[1] );


    free(pha);
    free(phb);
    free(phc);
}

void OnMultLineRange(int& EventSet, int startSize, int endSize, int interval){
	int current_size = startSize;
	char filename[MAX_PATH_SIZE];

	sprintf(filename, "./data/mul_line_%d_%d.csv", startSize, endSize);

	ofstream outfile;
   	outfile.open(filename);

	char* text;

	text = (char*)malloc(STRING_MAX * sizeof(char));

	outfile<<"Size, Time, L1_DCM, L2_DCM\n";

	while(current_size<=endSize){
		OnMultLine(EventSet, current_size, current_size, text);
		outfile<<text;
		cout<< text;
		current_size+= interval;
	}
	
	outfile.close();

	free(text);
}



// add code here for block x block matriz multiplication
void OnMultBlock(int& EventSet,int m_ar, int m_br, int bkSize, char* st)
{
	long long values[NUMBER_EVENTS];
    
	SYSTEMTIME Time1, Time2;
	
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

		
    pha = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phb = (double *)malloc((m_ar * m_ar) * sizeof(double));
	phc = (double *)malloc((m_ar * m_ar) * sizeof(double));

	for(i=0; i<m_ar; i++)
		for(j=0; j<m_ar; j++)
			pha[i*m_ar + j] = (double)1.0;



	for(i=0; i<m_br; i++)
		for(j=0; j<m_br; j++)
			phb[i*m_br + j] = (double)(i+1);


	start_papi_counters(EventSet);
    Time1 = clock();


	for (int ii = 0; ii < m_ar; ii+=bkSize){
		for(int jj = 0; jj < m_br; jj+=bkSize){
			for(int kk = 0; kk < m_br; kk+=bkSize){
				for(int i = ii; i < ii + bkSize; ++i){
					for(int k = kk; k < kk + bkSize; ++k){
						for(int j = jj; j < jj + bkSize; ++j){
							phc[i * m_ar + j] += pha[i*m_ar+k] * phb[k*m_br+j];
						}
					}
				}
			}
		}
	}

	stop_papi_counters(EventSet, values);
	Time2 = clock();
	

	double operation_time =  (double)(Time2 - Time1) / CLOCKS_PER_SEC;
	sprintf(st, "%d, %d, %3.3f, %lld, %lld\n",m_ar, bkSize, operation_time, values[0], values[1] );

    free(pha);
    free(phb);
    free(phc);
}

void OnMultBlockRange(int& EventSet, int startSize, int endSize, int interval){
	int current_size = startSize;
	int current_block_size = BLOCK_START_SIZE;

	char filename[MAX_PATH_SIZE];

	sprintf(filename, "./data/mul_block_%d_%d.csv", startSize, endSize);

	ofstream outfile;
   	outfile.open(filename);

	char* text;

	text = (char*)malloc(STRING_MAX * sizeof(char));
	cout<<"Size, Block_Size, Time, L1_DCM, L2_DCM\n";

	while(current_size<=endSize){
		while(current_block_size <= BLOCK_END_SIZE)
		{
			OnMultBlock(EventSet, current_size, current_size, current_block_size, text);
			cout<< text;
			outfile<<text;
			current_block_size*=2;
			
		}
		current_size+= interval;
		current_block_size = BLOCK_START_SIZE;
		
	}
	outfile.close();
	free(text);
}


//                          Main


int main (int argc, char *argv[])
{

	char c;
	int lin, col, blockSize;
	int startSize, endSize, interval;
	int op;
	char* text;


	
	int EventSet = PAPI_NULL;
  	long long values[2];
  	int ret;
	

	ret = PAPI_library_init( PAPI_VER_CURRENT );
	if ( ret != PAPI_VER_CURRENT )
		std::cout << "FAIL" << endl;


	ret = PAPI_create_eventset(&EventSet);
		if (ret != PAPI_OK) cout << "ERROR: create eventset" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L1_DCM );
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L1_DCM" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L2_DCM);
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L2_DCM" << endl;


	op=1;
	do {
		cout << endl << "1. Multiplication" << endl;
		cout << "2. Range Multiplication" << endl;
		cout << "3. Line Multiplication" << endl;
		cout << "4. Range Line Multiplication" << endl;
		cout << "5. Block Multiplication" << endl;
		cout << "6. Range Block Multiplication" << endl;
		cout << "Selection?: ";
		cin >>op;
		if (op == 0)
			break;
		else if(op%2==1){
			printf("Dimensions: lins=cols ? ");
   			cin >> lin;
   			col = lin;
			cout<< endl;
			text = (char*)malloc(STRING_MAX * sizeof(char));
		}
		else if(op != 6){
			printf("Start size for matrix: ");
			cin >> startSize;
			printf("End Size for matrix: ");
			cin>> endSize;
			printf("Interval for matrx size:");
			cin >>interval;
			cout<<endl;
		}

		switch (op){
			case 1: 
				cout<<"Size, Time, L1_DCM, L2_DCM\n";
				OnMult(EventSet, lin, col, text);
				cout<<text<<endl;
				free(text);
				break;
			case 2: 
				OnMultRange(EventSet, startSize, endSize, interval);
				break;
			case 3: 
				cout<<"Size, Time, L1_DCM, L2_DCM\n";
				OnMultLine(EventSet,lin, col, text);
				cout<<text<<endl;
				free(text);
				break;
			case 4:
				OnMultLineRange(EventSet, startSize, endSize, interval);  
				break;
			case 5:
				cout << "Block Size? ";
				cin >> blockSize;

				cout<<"Size, Block_Size, Time, L1_DCM, L2_DCM\n";
				OnMultBlock(EventSet, lin, col, blockSize, text);  
				cout<<text<<endl;
				free(text);
				break;
			case 6:
				OnMultBlockRange(EventSet, BLOCK_MUL_MIN, BLOCK_MUL_MAX,  BLOCK_MUL_INTERVAL);
				break;

		}




	}while (op != 0);

	ret = PAPI_remove_event( EventSet, PAPI_L1_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_remove_event( EventSet, PAPI_L2_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_destroy_eventset( &EventSet );
	if ( ret != PAPI_OK )
		std::cout << "FAIL destroy" << endl;

}