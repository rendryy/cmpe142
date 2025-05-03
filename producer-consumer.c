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


    if (empty == SEM_FAILED && errno == EEXIST) 
    {
        empty = sem_open("/my_semaphore", 0); // Open existing
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
        
        printf("Producer finished. Start consumer to process the data.\n");
        return 0; // Exit without calling cleanup()
    }
    
    //consumer for shared memory
    if(is_con && s_arg)
    {
        create_sharedmem(q_depth);
        consumer_shared(q_depth, e_arg);
        cleanup(); // Only consumer does full cleanup
    }
    // Only call cleanup here for unix socket mode
    else if (u_arg) {
        cleanup();
    }
    
    cleanup();
    return 0;
}

//producer function for unix sockets
void producer_socket(bool e, const char *m, int q)
{
    //UNIX domain socket 
    struct sockaddr_un addr;
    //queue size 
    for(int i = 0; i < q; i++)
    {
        int prod_fd;
        //set socket address
        memset(&addr, 0, sizeof(struct sockaddr_un));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, SOCKET_NAME, sizeof(addr.sun_path) - 1);
        char buffer[BUFFER_SIZE];
        //socket creation
        //loop for connection attempt if producer is ran first to retry connection until consumer
        while (1) 
        {
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
                //sleep(1) so while its waiting to connect to consumer, it doesn't spam the terminal
                sleep(1);
                close(prod_fd);
            }
            //if connected, break out of retry loop
            else
            {
                break;
            }
        }

        //send message
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
    char buffer[BUFFER_SIZE];
    //socket creation
    if((prod_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    //set socket address
    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;

    strncpy(addr.sun_path, SOCKET_NAME, sizeof(addr.sun_path) - 1);
    
    unlink(SOCKET_NAME);

    if(bind(prod_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_un)))
    {
        perror("Bind failed");
        close(prod_fd);
        exit(EXIT_FAILURE);
    }

    if(listen(prod_fd, 5) == -1)
    {
        perror("Listen failed");
        close(prod_fd);
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < q; i++)
    {
        con_fd = accept(prod_fd, NULL, NULL);
        if(con_fd == -1)
        {
            perror("Accept failed");
            close(prod_fd);
            exit(EXIT_FAILURE);
        }
        memset(buffer, 0, BUFFER_SIZE);
    
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


//function to create section of shared memory
void create_sharedmem(int q)
{
    int shm_fd = shm_open(SHM_NAME, O_CREAT| O_RDWR, 0666);
    if (shm_fd == -1) 
    {
        perror("shm_open failed");
        exit(EXIT_FAILURE);
    }
    //total_size is adjusted based on queue size
    size_t total_size = sizeof(queue_t) + (q * BUFFER_SIZE);
    struct stat shm_info;

    // If shared memory exists with wrong size, recreate it
    if (shm_info.st_size > 0 && shm_info.st_size != total_size) 
    {
        printf("Recreating shared memory with new size...\n");
        if (ftruncate(shm_fd, 0) == -1) {  // Truncate to 0
            perror("ftruncate(0) failed");
            exit(EXIT_FAILURE);
        }
        if (ftruncate(shm_fd, total_size) == -1) {
            perror("ftruncate(new size) failed");
            exit(EXIT_FAILURE);
        }
    } else if (shm_info.st_size == 0) 
    {
        // New shared memory
        if (ftruncate(shm_fd, total_size) == -1) 
        {
            perror("ftruncate failed");
            exit(EXIT_FAILURE);
        }
    }

    q_t = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (q_t == MAP_FAILED) 
    {
        perror("mmap failed");
        exit(EXIT_FAILURE);
    }
    sem_wait(mutex);
    if(q_t->running == 0)
    {
        q_t->head = 0;
        q_t->tail = 0;
        q_t->q_size = q;
        q_t->count = 0;
        q_t->running = 1;
        for (int i = 0; i < q; i++) 
        {
            memset(&q_t->messages[i * BUFFER_SIZE], 0, BUFFER_SIZE);
        }

    }
    else if (q != q_t->q_size) 
    {
        fprintf(stderr, "Mismatch: Shared memory already initialized with q_size = %d, but received q = %d\n", q_t->q_size, q);
        sem_post(mutex);
        cleanup();
        exit(EXIT_FAILURE);
    }
    q_t->count++;
    sem_post(mutex);
}

//function for producer in shared memory, iterates through queue size and produces messages 
void producer_shared(const char *m, int q, bool e)
{
    for(int i = 0; i < q; i++)
    {
        sem_wait(empty);
        sem_wait(mutex);

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
}

//function for consumer in shared memory, iterarates through queue size and consumes messages in shared memory
void consumer_shared(int q, bool e)
{
    for(int i = 0; i < q; i++)
    {
        sem_wait(full);
        sem_wait(mutex);
        char m[BUFFER_SIZE];

        strncpy(m, &q_t->messages[q_t->tail * BUFFER_SIZE], BUFFER_SIZE - 1);

        m[BUFFER_SIZE - 1] = '\0';
        q_t->tail = (q_t->tail + 1) % q_t->q_size;
        if (e) 
        {
            printf("Consumer Received: %s\n", m);
        }
        sem_post(mutex);
        sem_post(empty);
    }
}


//function to cleanup semaphores after program runs 
void cleanup()
{
    // Check if q_t is initialized (only happens in shared memory mode)
    if (q_t != NULL) {
        sem_wait(mutex);
        q_t->count--;
        int should_clean = (q_t->count == 0);
        sem_post(mutex);

        if (should_clean) {
            printf("Cleaning up shared memory resources...\n");
            size_t total_size = sizeof(queue_t) + (q_t->q_size * BUFFER_SIZE);
            munmap(q_t, total_size);
            shm_unlink(SHM_NAME);
            
            // Also unlink semaphores since we're the last process
            sem_unlink(SEM_FULL);
            sem_unlink(SEM_EMPTY);
            sem_unlink(SEM_MUTEX);
        }
        
        // Close the semaphores in any case
        sem_close(full);
        sem_close(empty);
        sem_close(mutex);
    } else {
        // For unix socket mode, close and unlink semaphores
        sem_close(full);
        sem_close(empty);
        sem_close(mutex);
        sem_unlink(SEM_FULL);
        sem_unlink(SEM_EMPTY);
        sem_unlink(SEM_MUTEX);
    }
}