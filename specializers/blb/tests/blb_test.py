import unittest

from blb import BLB

class SVMVerifierBLB(BLB):
    def compute_estimate(emails, models):
        errors =0.0
        for email in emails:
            weight = email[0]
            tag = email[1][0][1]
            choice = 0
            max_match = -1.0
            for i in range(len(models)):
                model = models[i]
                total = custom_dot(model, email)
                if total > max_match:
                    choice = i + 1
                    max_match = total    
            if choice != tag:
                errors += 1 * weight 
        return errors / len (emails)

    def reduce_bootstraps(bootstraps):
        mean = 0.0
        for bootstrap in bootstraps:
            mean += bootstrap
        return mean / len(bootstraps)

    def average(subsamples):
        mean = 0.0
        for subsample in subsamples:
            mean += subsample
        return mean/len(subsamples)


class SVMVerifierBLBTest(unittest.TestCase):
    mean = "return arr.reduce(_+_)/arr.length"
    sd = """val mean = arr.reduce(_+_)/arr.length
            return math.sqrt(arr.map(x => (x-mean)*(x-mean)).reduce(_+_))/(arr.length - 1)
    """

    def test(self):
        data = tuple([i*1.0 for i in xrange(5000)])
        test_blb = SVMVerifierBLB(25, 50, .5, use_scala=True)    
           
        result = test_blb.run('/root/enron/test.dat',\
                               '/root/enron/model')
        print 'FINAL RESULT IS:', result  
        self.assertTrue(abs(result - len(data)/2) < len(data)/90)

if __name__ == '__main__':
    unittest.main()
