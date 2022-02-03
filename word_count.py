'''
program that processes the word count in a data set
'''
from multiprocessing import Process, Queue
from collections import Counter



'''
given a list of sentences, this counts the words in them
'''
def word_counts(sentences):
    count = Counter()
    for line in sentences:
        words = line.strip().lower().split(' ')
        for word in words:
            if word:
                count[word] +=1

    return count


'''
reading the file, taking the slice, then sending it one slice/chunk 
at a time to the method that counts it
'''
if __name__ == '__main__':
    with open("AliceInWonderland.txt") as fp:
        lines = fp.readlines()


        slice = len(lines) // 3
        count = Counter()
        start = 0
        threads = []
        for i in range(3):
            p = Process(target=word_counts, args=[lines[start:start+slice]])
            start += slice
            threads.append(p)
            p.start()

        for i in range(3):
            count.update(q.get())

        for i in range(3) :
            p.join()
            

        print(count.most_common(3))
        print(count.most_common()[:-4:-1])

        print(word_counts(lines))