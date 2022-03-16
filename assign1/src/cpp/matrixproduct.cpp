#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <cstdlib>
#include <papi.h>

using namespace std;

#define SYSTEMTIME clock_t
/**
 * @brief Number os PAPI events
 * 
 */
#define NUMBER_EVENTS 2

#define STRING_MAX 256


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

	/*
	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;
	*/

    free(pha);
    free(phb);
    free(phc);
	
	
}

void OnMultRange(int& EventSet, int startSize, int endSize, int interval){
	int current_size = startSize;

	char* text;

	text = (char*)malloc(STRING_MAX * sizeof(char));

	while(current_size<=endSize){
		OnMult(EventSet, current_size, current_size, text);
		cout<< text;
		current_size+= interval;
	}

	free(text);
}

// add code here for line x line matriz multiplication
void OnMultLine(int m_ar, int m_br)
{
    SYSTEMTIME Time1, Time2;
	
	char st[100];
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


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC);
	cout << st;

	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
}

// add code here for block x block matriz multiplication
void OnMultBlock(int m_ar, int m_br, int bkSize)
{
    
	SYSTEMTIME Time1, Time2;
	
	char st[100];
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


	Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC);
	cout << st;

	// display 10 elements of the result matrix tto verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,m_br); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
}




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
		else{
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
				OnMult(EventSet, lin, col, text);
				cout<<text<<endl;
				break;
			case 2: 
				OnMultRange(EventSet, startSize, endSize, interval);
				break;
			case 3: 
				//OnMult(lin, col);
				break;
			case 4:
				OnMultLine(lin, col);  
				break;
			case 5:
				cout << "Block Size? ";
				cin >> blockSize;
				OnMultBlock(lin, col, blockSize);  
				break;
			case 6:
				//onMultiBlock()
				break;

		}

		free(text);


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