import java.io.*;

public class line_count{
public static int count(String filename) throws IOException {
    InputStream is = new BufferedInputStream(new FileInputStream(filename));
    try {
        byte[] c = new byte[1024];
        int count = 0;
        int readChars = 0;
        while ((readChars = is.read(c)) != -1) {
            for (int i = 0; i < readChars; ++i) {
                if (c[i] == '\n')
                    ++count;
            }
        }
        return count;
    } finally {
        is.close();
    }
}

public static void main(String[] arg) throws IOException{
	System.out.println("lines are:" + count("/media/sf_share/users/pbirsinger/documents/research/asp_scala/asp_git/specializers/blb/data/example/test.dat"));
	}
}
