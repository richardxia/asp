import unittest

from blb import BLB

"""
should create an "email" class in python as well
"""
from avroInter.PyAvroInter import *

class SVMVerifierBLB(BLB):
    """
    def compute_estimate(emails, models):
        errors =0.0
        num_emails = 0
        for email in emails:
            weight = email.get_weight()
            num_emails += weight
            tag = email.get_tag()
            choice = 0
            max_match = -1.0
            for i in range(len(models)):
                model = models[i]
                total = custom_dot(model, email)
                if total > max_match:
                    choice = i + 1
                    max_match = total    
            if choice != tag:
                errors += weight 
                
        return errors / num_emails
    """
    def compute_estimate(emails, num_classes):
        errors =0.0
        num_emails = 0
        model_reader = read_avro_file('p113kmodel.avro')
        models = model_iter.next()
        for email in emails:
            weight = email.get_weight()
            num_emails += weight
            tag = email.get_tag()
            choice = 0
            max_match = -1.0
            for i in range(num_classes):
                model = models.get(i+1)
                total = custom_dot(model, email)
                if total > max_match:
                    choice = i + 1
                    max_match = total    
            if choice != tag:
                errors += weight 
                
        return errors / num_emails
    
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
           
        result = test_blb.run('s3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeEmail/',\
                               's3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeModel/')
        print 'FINAL RESULT IS:', result  
        self.assertTrue(abs(result - len(data)/2) < len(data)/90)

if __name__ == '__main__':
    unittest.main()
