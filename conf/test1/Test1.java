import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cachekit.CacheKit;
import com.cachekit.access.CacheKitAccess;

public class Test1
{
	private static final Log log = LogFactory.getLog(Test1.class);

	public static void main(String[] args) throws InterruptedException
	{
		System.out.println(log.isDebugEnabled());

		System.out.println(log.isInfoEnabled());

		CacheKitAccess cacheKitAccess = CacheKit.getInstance("default");

		//main线程休眠，等待服务发现和注册的完成
		Thread.sleep(6000);

		int max = 30;

		for (int i = 0; i < max; i++)
		{
			cacheKitAccess.put("id" + i, i);
		}
		System.out.println("缓存对象" + cacheKitAccess.get("id0"));

		//main线程休眠，等待其他节点的读取功能完成
		Thread.sleep(30000);
	}
}
