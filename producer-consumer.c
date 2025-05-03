#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <errno.h>
#include <getopt.h>
#include <sys/stat.h>


//constant definitions
#define SOCKET_NAME "/tmp/pc.sock"
#define BUFFER_SIZE 1024
#define SHM_NAME "/pc_shm"
#define SEM_FULL "/sem_full"
#define SEM_EMPTY "/sem_empty"
#define SEM_MUTEX "/sem_mutex"

sem_t *full;
sem_t *empty;
sem_t *mutex;

//queue struct to store messages and manage producer-consumer shared memory
typedef struct
{
    int head;
    int tail;
    int q_size;
    int count;
    int running;
    int done;
    char messages[BUFFER_SIZE];
}queue_t;

queue_t *q_t;


void producer_socket(bool e, const char *m, int q);
void consumer_socket(bool e, int q);
void producer_shared(const char *m, int q, bool e);
void consumer_shared(int q, bool e);
void cleanup();
void create_sharedmem(int q);


int main(int argc, char *argv[])
{
    //flags to keep track of cli arguments 
    bool is_prod = false;
    bool is_con = false;
    bool msg_passed = false;
    bool queue_arg = false;
    bool u_arg = false;
    bool s_arg = false;
    bool e_arg = false;
    char c;
    int q_depth = 0;
    char msg[BUFFER_SIZE] = {0};
    //parse cli arguments with error handling 
    while((c =getopt(argc, argv, "pcm:q:use")) != -1)
    {
        switch(c)
        {
            case 'p':
                if(is_prod)
                {
                  fprintf(stderr, "Error: Multiple -p Arguements Passed");
                  exit(EXIT_FAILURE);
                }
                is_prod = true;
                break;

            case 'c':
                if(is_con)
                {
                  fprintf(stderr, "Error: Multiple -c Arguements Passed");
                  exit(EXIT_FAILURE);
                }
                is_con = true;
                break;

            case 'q':
                if(queue_arg)
                {
                  fprintf(stderr, "Error: Multiple Arguments Passed");
                  exit(EXIT_FAILURE);
                }
                queue_arg = true;
                q_depth = atoi(optarg);
                break;

            case 'u':
                if(u_arg)
                {
                    fprintf(stderr, "Error: Multiple -u Arguments Passed\n");
                    exit(EXIT_FAILURE);
                }
                u_arg = true;
                break;
            case 's':
                if(s_arg)
                {
                    fprintf(stderr, "Error: Multiple -s Arguments Passed\n");
                    exit(EXIT_FAILURE);
                }
                s_arg = true;

                break;
            case 'e':
                if(e_arg)
                {
                    fprintf(stderr, "Error: Multiple -e Arguments Passed \n");
                    exit(EXIT_FAILURE);
                }
                e_arg = true;
                break;
            
            case 'm':
                if(msg_passed)
                {
                  fprintf(stderr, "Error: Multiple -m  Arguements Passed");
                  exit(EXIT_FAILURE);
                }
                msg_passed = true;

                strncpy(msg, optarg, BUFFER_SIZE - 1);
                break;
            default:
                fprintf(stderr, "Usage: %s -p/-c -q <depth> -u/-s -e -m <message>\n ", argv[0]);

        }
    }
    //create semaphores
    full = sem_open(SEM_FULL, O_CREAT, 0666, 0);
    empty = sem_open(SEM_EMPTY, O_CREAT, 0666, q_depth); 
    mutex = sem_open(SEM_MUTEX, O_CREAT, 0666, 1);

    //error handling for semaphores
    //check for existing semaphores
    if (empty == SEM_FAILED && errno == EEXIST) 
    {
        empty = sem_open("/my_semaphore", 0); 
    }

    if (mutex == SEM_FAILED || full == SEM_FAILED || empty == SEM_FAILED) 
    {
        perror("sem_open failed");
        exit(EXIT_FAILURE);
    }

    //error handling for aguments passed
    if ((is_prod && is_con) || (!is_prod && !is_con) )
    {
        fprintf(stderr, "Error: Please enter either -p or -c\n");
        exit(EXIT_FAILURE);
    }
    if ((u_arg && s_arg) || (!u_arg && !s_arg))
    {
        fprintf(stderr, "Error: Please enter either -u or -s\n");
        exit(EXIT_FAILURE);
    }
    //producer for unix socket
    if(is_prod && u_arg)
    {
        if(!msg_passed)
        {
            fprintf(stderr, "Error: -p requires -m \n");
            exit(EXIT_FAILURE);
        }
        producer_socket(e_arg, msg, q_depth);
        cleanup();

    }
    //consumer for unix socket
    if(is_con && u_arg)
    {
        consumer_socket(e_arg,q_depth);
    }
    //shared memory creation
    if(s_arg)
    {
        create_sharedmem(q_depth);
    }

    //producer for shared memory
    if(is_prod && s_arg)
    {
        if(!msg_passed)
        {
            fprintf(stderr, "Error: -p requires -m\n ");
            exit(EXIT_FAILURE);
        }
        create_sharedmem(q_depth);
        producer_shared(msg, q_depth, e_arg);
        
        // Only close the semaphores but don't unlink them
        sem_close(full);
        sem_close(empty);
        sem_close(mutex);
    }
    
    //consumer for shared memory
    if(is_con && s_arg)
    {
        create_sharedmem(q_depth);
        consumer_shared(q_depth, e_arg);
        cleanup(); 
    }
    // 
    else if (u_arg) {
        cleanup();
    }
    
    cleanup();
    return 0;
}

//producer function for unix sockets
void producer_socket(bool e, const char *m, int q)
{
    //UNIX domain socket address struct
    struct sockaddr_un addr;
    //queue size 
    for(int i = 0; i < q; i++)
    {
        int prod_fd;
        //set socket address
        memset(&addr, 0, sizeof(struct sockaddr_un));
        //set to UNIX socket domain
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCKET_NAME, sizeof(addr.sun_path) - 1);
        char buffer[BUFFER_SIZE];
        //socket creation
        //retry loop 
        while (1) 
        {
            //create socket
            prod_fd = socket(AF_UNIX, SOCK_STREAM, 0);
            if (prod_fd < 0) 
            {
                perror("Producer: socket failed");
                exit(EXIT_FAILURE);
            }

            //connect to consumer
            if(connect(prod_fd, (const struct sockaddr *) &addr, sizeof(struct sockaddr_un)) == -1)
            {
                perror("Connect failed, waiting for consumer");
                //delay to prevent terminal spam from continuous retries
                sleep(1);
                close(prod_fd);
            }
            //once connected, exit retry loop as it has a connection
            else
            {
                break;
            }
        }

        //send message through socket
        if(write(prod_fd, m, strlen(m)) < 0)
        {
            perror("Write failed");
            close(prod_fd);
            exit(EXIT_FAILURE);
        }
        //if e argument is passed by user
        if(e)
        {
            printf("Message from Producer: %s\n", m);
        }
        close(prod_fd);
  }
}

//consumer function for unix sockets
void consumer_socket(bool e, int q)
{
    int prod_fd, con_fd;
    struct sockaddr_un addr;
    //message buffer
    char buffer[BUFFER_SIZE];
    //socket creation
    if((prod_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    //initialize scocket address struct and Unix Socket
    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_NAME, sizeof(addr.sun_path) - 1);
    
    //remove existing sockets
    unlink(SOCKET_NAME);
    //bind socket to address
    if(bind(prod_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_un)))
    {
        perror("Bind failed");
        close(prod_fd);
        exit(EXIT_FAILURE);
    }
    //listen for connections
    if(listen(prod_fd, 5) == -1)
    {
        perror("Listen failed");
        close(prod_fd);
        exit(EXIT_FAILURE);
    }
    //iterate through queue
    for (int i = 0; i < q; i++)
    {
        //accept incoming connection
        con_fd = accept(prod_fd, NULL, NULL);
        if(con_fd == -1)
        {
            perror("Accept failed");
            close(prod_fd);
            exit(EXIT_FAILURE);
        }
        //clear buffer
        memset(buffer, 0, BUFFER_SIZE);
        //read message
        if(read(con_fd, buffer, BUFFER_SIZE - 1) > 0 )
        {
            if(e)
            {
                printf("Consumer received: %s\n", buffer);
            }
            else
            {
                perror("Read failed");
            }
        }
    }

    close(con_fd);
    close(prod_fd);
    unlink(SOCKET_NAME);
}


//function to create shared memory 
void create_sharedmem(int q)
{
    //open shared memory segment
    int shm_fd = shm_open(SHM_NAME, O_CREAT| O_RDWR, 0666);
    if (shm_fd == -1) 
    {
        perror("shm_open failed");
        exit(EXIT_FAILURE);
    }
    
    //check for existing shared memory size 
    struct stat shm_stat;
    if (fstat(shm_fd, &shm_stat) == -1) {
        perror("fstat failed");
        exit(EXIT_FAILURE);
    }
    
    int queue = 0;
    size_t req_size = sizeof(queue_t) + (q * BUFFER_SIZE);
    size_t size = shm_stat.st_size;
    
    // If shared memory already exists, determine its queue size
    if (size > sizeof(queue_t)) 
    {
        queue_t *temp = mmap(NULL, sizeof(queue_t), PROT_READ, MAP_SHARED, shm_fd, 0);
        if (temp == MAP_FAILED) 
        {
            perror("mmap failed during size check");
            exit(EXIT_FAILURE);
        }
        queue = temp->q_size;
        munmap(temp, sizeof(queue_t));
        
        //determine max queue size 
        int max_q_size;
        if (queue > q)
        {
            max_q_size = queue;
        }
        else
        {
            max_q_size = q;
        }

        req_size = sizeof(queue_t) + (max_q_size * BUFFER_SIZE);
    }

    //resize if needed 
    if (req_size > size) 
    {
        if (ftruncate(shm_fd, req_size) == -1) 
        {
            perror("ftruncate failed");
            exit(EXIT_FAILURE);
        }
    }
    //map shared memory 
    q_t = mmap(NULL, req_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (q_t == MAP_FAILED) 
    {
        perror("mmap failed");
        exit(EXIT_FAILURE);
    }
    //initialize shared memory
    sem_wait(mutex);
    //if running for first time, initialize 
    if(q_t->running == 0)
    {
        q_t->head = 0;
        q_t->tail = 0;
        q_t->q_size = q;
        q_t->count = 0;
        q_t->running = 1;
        //initalize all message slots
        for (int i = 0; i < q; i++) 
        {
            memset(&q_t->messages[i * BUFFER_SIZE], 0, BUFFER_SIZE);
        }

    }
    else if (q > q_t->q_size) 
    {     
        // initialize new message slots
        for (int i = q_t->q_size; i < q; i++) 
        {
            memset(&q_t->messages[i * BUFFER_SIZE], 0, BUFFER_SIZE);
        }
        
        q_t->q_size = q;
    }
    //increment process count 
    q_t->count++;
    sem_post(mutex);
}

void producer_shared(const char *m, int q, bool e)
{
    //iterate for q size 
    for(int i = 0; i < q; i++)
    {
        sem_wait(empty);
        sem_wait(mutex);
        //copy message into queue and update pointer head 
        strncpy(&q_t->messages[q_t->head * BUFFER_SIZE], m, BUFFER_SIZE - 1);
        q_t->messages[q_t->head * BUFFER_SIZE + BUFFER_SIZE - 1] = '\0';
        q_t->head = (q_t->head +1) % q_t->q_size;
        if (e) 
        {
            printf("Message from Producer: %s\n", m);
        }
        sem_post(mutex);
        sem_post(full);

    }
    //signal completion
    sem_wait(mutex);
    q_t->done = 1;
    sem_post(mutex);
}

void consumer_shared(int q, bool e)
{
    //iterate to consume q messages
    for(int i = 0; i < q; i++)
    {
        //wait for item and lock queue
        sem_wait(full);       
        sem_wait(mutex);      

        // Verify tail is within bounds
        if (q_t->tail >= q_t->q_size) {
            fprintf(stderr, "Error: Invalid tail position %d (q_size=%d)\n", 
                    q_t->tail, q_t->q_size);
            exit(EXIT_FAILURE);
        }

        // Calculate message position 
        char *msg_ptr = &q_t->messages[q_t->tail * BUFFER_SIZE];
        
        // Copy message
        char m[BUFFER_SIZE];
        strncpy(m, msg_ptr, BUFFER_SIZE - 1);
        m[BUFFER_SIZE - 1] = '\0';  // Ensure null-termination

        // Update tail 
        q_t->tail = (q_t->tail + 1) % q_t->q_size;

        if (e) 
        {
            printf("Consumer Received: %s\n", m);
        }
        
        sem_post(mutex);     
        sem_post(empty);   
    }
}

//clean up everything 
void cleanup()
{
    // Check if q_t was initialized
    if (q_t != NULL) 
    {
        //update process count
        sem_wait(mutex);
        q_t->count--;
        //only clean if process count has reached 0
        int clean = (q_t->count == 0);
        sem_post(mutex);
        //unlink everything
        //if last process, clean everything
        if (clean) 
        {
            size_t size = sizeof(queue_t) + (q_t->q_size * BUFFER_SIZE);
            munmap(q_t, size);
            shm_unlink(SHM_NAME);
            
            sem_unlink(SEM_FULL);
            sem_unlink(SEM_EMPTY);
            sem_unlink(SEM_MUTEX);
        }
        
        //close semaphores
        sem_close(full);
        sem_close(empty);
        sem_close(mutex);
    } 
    else 
    {
        // For unix socket mode, close and unlink semaphores
        sem_close(full);
        sem_close(empty);
        sem_close(mutex);
        sem_unlink(SEM_FULL);
        sem_unlink(SEM_EMPTY);
        sem_unlink(SEM_MUTEX);
    }
}