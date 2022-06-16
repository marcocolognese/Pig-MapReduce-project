# Pig MapReduce Project

Lo scopo principale di questo progetto è quello di mettere a confronto ***Pig Latin*** e ***MapReduce***. \
\
Per fare ciò è stato deciso di svolgere quattro esercizi basati su un dataset contenente informazioni sugli arrivi e sulle partenze negli aeroporti situati negli Stati Uniti eseguendo quindi delle specifiche analisi su tali dati. Ognuna di queste è stata implementata sia in Pig Latin che in MapReduce e su questa relazione ne sono stati riportati rispettivamente il codice e lo pseudocodice entrambi preceduti da una spiegazione. \
\
Oltre a questo, alla fine di ogni esercizio vengono svolte delle analisi e delle statistiche riguardanti le due run, riportando dunque informazioni relative alle fasi di Map e di Reduce ed allo scambio di dati intermedi tra le due. In particolare il numero di reducer può essere indicato nel codice di default è uno. \
\
Sia nel caso di Pig Latin che di MapReduce, il codice dei quattro esercizi è stato prima testato nel container presente nel proprio calcolatore e successivamente in un cluster vero e proprio messo a disposizione dal docente. Questo è stato fatto per analizzare le eventuali differenze che ci possono essere tra l’utilizzo di un ambiente di simulazione ed uno reale. \
\
Infine, nell’ultimo capitolo, si parla delle competenze acquisite da parte degli studenti grazie allo sviluppo di questo progetto, delle difficoltà incontrate, delle soluzioni adottate e dei vantaggi e svantaggi relativi a Pig Latin e MapReduce. \
\
Nell’implementazione del codice dei quattro esercizi, nella parte riguardante Pig Latin, è sta- to deciso di utilizzare come nome utente "ross". Dunque, nel caso in cui si volessero eseguire tali codici sul proprio calcolatore, occorre modificarli sostituendo la stringa "ross" con il proprio nome utente. \
\
Tale problema è invece assente per quanto riguarda MapReduce, dove il nome utente va specificato scrivendolo a linea di comando solamente quando si esegue il codice.

## Dataset
I dati utilizzati come input per i quattro esercizi sono stati recuperati dalla seguente pagina web: http://stat-computing.org/dataexpo/2009/the-data.html.  
I file scaricabili contengono informazioni strutturate riguardanti i voli aerei negli Stati Uniti e sono composti da 29 campi, in particolare quelli utilizzati sono:
- *Year* (campo numero 1): anno in cui è stato effettuato il volo;
- *DayOfWeek* (campo numero 4): giorno della settimana in cui è stato effettuato il volo, dove 1 corrisponde a lunedì e 7 a domenica;
- *FlightNum* (campo numero 10): numero del volo;
- *ArrDelay* (campo numero 15): differenza in minuti tra l’orario di arrivo previsto e quello effettivo, gli arrivi in anticipo sono indicati con numeri negativi;
- *WeatherDelay* (campo numero 26): ritardo in minuti del volo dovuto al tempo meteorologico.

I file a disposizione sul sito web sono suddivisi per anno, in particolare dal 1987 al 2008. Quelli utilizzati in questo progetto sono quelli compresi tra:
- il 1987 ed il 1990;
- il 2005 ed il 2008.
\
Infine, per quanto riguarda il problema dei quattro esercizi svolti e di seguito esposti, è stato deciso di prendere spunto da quelli descritti nella pagina http://stat-computing.org/dataexpo/2009/ sotto la voce "The challenge".

## Cluster
Un cluster è un insieme di computer connessi tra loro tramite una rete. Lo scopo principale è quello di distribuire un’elaborazione molto complessa tra i vari calcolatori a disposizione. \
Ovvero, un problema che richiede molte elaborazioni, per essere risolto viene scomposto in sotto- problemi più piccoli i quali vengono risolti in parallelo. Questo ovviamente aumenta la potenza di calcolo del sistema. \
\
I cluster hanno le seguenti caratteristiche:
- i vari calcolatori vengono visti come una singola risorsa computazionale;
- il server cluster possiede alte prestazioni poiché, invece di gravare su un’unica macchina standalone, suddivide il carico di lavoro su più macchine.

Per l’utente, il cluster è assolutamente trasparente, cioè tutta la notevole complessità, sia hardware che software, viene mascherata. \
\
Il cluster utilizzato è stato messo a disposizione dal docente e, per accedere, occorre essere connessi alla rete dell’Ateneo di Verona. \
Il login tramite ssh alla macchina master viene effettuato utilizzando come nome utente "student", ovvero attraverso il comando ssh student@hadoop-compute0.di.univr.it.
\
È anche possibile controllare come procede il processing sul cluster accedendo da interfaccia web attraverso l’url http://hadoop-compute0.di.univr.it:50030. Vengono cioè mostrate varie in- formazioni relative ai Map, ai Reduce ed ai job in esecuzione, completati e falliti. \
Questo cluster è composto da quattro nodi, ovvero quattro calcolatori. Ognuno di questi può eseguire al massimo due map task e due reduce task, dunque la capacità massima del cluster utilizzato corrisponde a:
- 8 map task;
- 8 reduce task.