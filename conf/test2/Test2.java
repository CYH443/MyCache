import com.cachekit.CacheKit;
import com.cachekit.access.CacheKitAccess;

public class Test2 {
    public static void main(String[] args) throws InterruptedException {

        CacheKitAccess cacheKitAccess = CacheKit.getInstance("default");
        //main线程休眠，等待服务发现和注册的完成
        Thread.sleep(12000);

        //测试节点的数据读取
        for (int i = 0; i < 25; i++)
        {
            System.out.println("缓存对象" + cacheKitAccess.get("id" + i));
        }

        Thread.sleep(30000);
    }
}
