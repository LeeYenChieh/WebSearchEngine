import { useState, useEffect, useMemo } from 'react';
import { format, parseISO, startOfWeek, startOfMonth } from 'date-fns';

// Types
import type { RawData, ServerDailyStat, TimeFrame, FailReasonChartData, FailureReasons } from '../types';
import { CRAWLER_URL, TYPESENSE_URL } from '../url';

// Containers
import StatusCardContainer from './StatusCardContainer';

// Components
import DashboardHeader from '../Components/DashboardHeader';
import TotalVolumeChart from '../Components/TotalVolumeChart';
import DailyTrendsChart from '../Components/DailyTrendsChart';
import FailReasonsChart from '../Components/FailReasonsChart';
import MetricTrendChart from '../Components/MetricTrendChart';

// 定義 Metric 的資料結構
interface MetricRawData {
  __total__: {
    discover_find: number;
    fetch_find: number;
    upload_find: number;
    rank_find: number;
    total: number;
  }
}

// 處理後的圖表用資料結構
interface ProcessedMetric {
  date: string;
  total: number;
  discover_find: number;
  fetch_find: number;
  upload_find: number;
  rank_find: number;
  // 百分比欄位供圖表 Y 軸使用
  discover_pct: number;
  fetch_pct: number;
  upload_pct: number;
  rank_pct: number;
}

export default function App() {
  const [rawData, setRawData] = useState<RawData | null>(null);
  const [timeFrame, setTimeFrame] = useState<TimeFrame>('daily');
  
  // 新增 State 來儲存 Head 和 Random 的資料
  const [headMetrics, setHeadMetrics] = useState<ProcessedMetric[]>([]);
  const [randomMetrics, setRandomMetrics] = useState<ProcessedMetric[]>([]);

  // --- 1. Data Fetching Logic (Main Status) ---
  useEffect(() => {
    fetch('/result/status.json')
      .then((res) => res.json())
      .then((data) => setRawData(data))
      .catch((err) => console.error('Failed to load status data:', err));
  }, []);

  // --- 1.1 Fetching Logic (Metrics) ---
  useEffect(() => {
    if (!rawData) return;

    const fetchMetrics = async () => {
      const dates = Object.keys(rawData).sort();
      
      const headResults: ProcessedMetric[] = [];
      const randomResults: ProcessedMetric[] = [];

      await Promise.all(
        dates.map(async (date) => {
          const fileDate = date.replace(/-/g, '');
          
          try {
            // Fetch Head Data
            const headRes = await fetch(`/result/head_all_${fileDate}.json`);
            if (headRes.ok) {
              const json: MetricRawData = await headRes.json();
              const t = json.__total__;
              headResults.push({
                date,
                total: t.total,
                discover_find: t.discover_find,
                fetch_find: t.fetch_find,
                upload_find: t.upload_find,
                rank_find: t.rank_find,
                discover_pct: t.total > 0 ? (t.discover_find / t.total) * 100 : 0,
                fetch_pct: t.total > 0 ? (t.fetch_find / t.total) * 100 : 0,
                upload_pct: t.total > 0 ? (t.upload_find / t.total) * 100 : 0,
                rank_pct: t.total > 0 ? (t.rank_find / t.total) * 100 : 0,
              });
            }

            // Fetch Random Data
            const randomRes = await fetch(`/result/random_all_${fileDate}.json`);
            if (randomRes.ok) {
              const json: MetricRawData = await randomRes.json();
              const t = json.__total__;
              randomResults.push({
                date,
                total: t.total,
                discover_find: t.discover_find,
                fetch_find: t.fetch_find,
                upload_find: t.upload_find,
                rank_find: t.rank_find,
                discover_pct: t.total > 0 ? (t.discover_find / t.total) * 100 : 0,
                fetch_pct: t.total > 0 ? (t.fetch_find / t.total) * 100 : 0,
                upload_pct: t.total > 0 ? (t.upload_find / t.total) * 100 : 0,
                rank_pct: t.total > 0 ? (t.rank_find / t.total) * 100 : 0,
              });
            }
          } catch (e) {
            console.warn(`Missing metric data for ${date}`);
            console.log(e)
          }
        })
      );

      setHeadMetrics(headResults.sort((a, b) => a.date.localeCompare(b.date)));
      setRandomMetrics(randomResults.sort((a, b) => a.date.localeCompare(b.date)));
    };

    fetchMetrics();
  }, [rawData]);


  // --- 2. Data Aggregation Logic (TimeFrame) ---
  const processedData: ServerDailyStat[] = useMemo(() => {
    if (!rawData) return [];

    const sortedDates = Object.keys(rawData).sort();

    const dailyBase: ServerDailyStat[] = sortedDates.map((date) => {
      const entry = rawData[date];
      return {
        stat_date: date,
        displayDate: date.slice(5),
        discovered: entry.discovered,
        crawled: entry.crawled,
        indexed: entry.indexed,
        new_links: entry.detail?.new_links,
        fetch_ok: entry.detail?.fetch_ok,
        fetch_fail: entry.detail?.fetch_fail,
        failed_rate: entry.detail?.failed_rate,
        fail_reasons: entry.detail?.fail_reasons,
      };
    });

    if (timeFrame === 'daily') return dailyBase;

    const groupedData: Record<string, ServerDailyStat[]> = {};

    dailyBase.forEach((item) => {
      const dateObj = parseISO(item.stat_date);
      const key =
        timeFrame === 'weekly'
          ? format(startOfWeek(dateObj), 'yyyy-MM-dd')
          : format(startOfMonth(dateObj), 'yyyy-MM-dd');

      if (!groupedData[key]) groupedData[key] = [];
      groupedData[key].push(item);
    });

    return Object.entries(groupedData).map(([key, group]) => {
      const lastEntry = group[group.length - 1];
      
      const sumField = (field: keyof ServerDailyStat) =>
        group.reduce((acc, curr) => acc + (curr[field] as number), 0);

      const aggregatedReasons: FailureReasons = {};
      group.forEach((g) => {
        if (g.fail_reasons) {
          Object.entries(g.fail_reasons).forEach(([rKey, rVal]) => {
            aggregatedReasons[rKey] = (aggregatedReasons[rKey] || 0) + rVal;
          });
        }
      });

      const totalFetchOk = sumField('fetch_ok');
      const totalFetchFail = sumField('fetch_fail');
      const totalFetch = totalFetchOk + totalFetchFail;

      return {
        stat_date: key,
        displayDate: timeFrame === 'weekly' ? `Week ${key.slice(5)}` : format(parseISO(key), 'MMM yyyy'),
        discovered: lastEntry.discovered,
        crawled: lastEntry.crawled,
        indexed: lastEntry.indexed,
        new_links: sumField('new_links'),
        fetch_ok: totalFetchOk,
        fetch_fail: totalFetchFail,
        failed_rate: totalFetch > 0 ? totalFetchFail / totalFetch : 0,
        fail_reasons: aggregatedReasons,
      };
    });
  }, [rawData, timeFrame]);

  // --- 3. Transformation Logic for FailReasonsChart ---
  const { failReasonChartData, failReasonKeys } = useMemo(() => {
    if (!processedData || processedData.length === 0) {
      return { failReasonChartData: [], failReasonKeys: [] };
    }

    const TOP_N_LIMIT = 5;
    const globalTotals: Record<string, number> = {};

    processedData.forEach((day) => {
      if (!day.fail_reasons) return;
      Object.entries(day.fail_reasons).forEach(([reason, count]) => {
        if (['fetch_fail', 'failed_rate'].includes(reason)) return;
        globalTotals[reason] = (globalTotals[reason] || 0) + (count || 0);
      });
    });

    const sortedReasons = Object.keys(globalTotals).sort((a, b) => globalTotals[b] - globalTotals[a]);
    const topKeys = sortedReasons.slice(0, TOP_N_LIMIT);
    const hasOthers = sortedReasons.length > TOP_N_LIMIT;
    const keys = hasOthers ? [...topKeys, 'Others'] : topKeys;

    const formattedData = processedData.map((day) => {
      const row: FailReasonChartData = { displayDate: day.displayDate };
      let othersCount = 0;
      if (day.fail_reasons) {
        Object.entries(day.fail_reasons).forEach(([reason, count]) => {
          if (topKeys.includes(reason)) {
            row[reason] = count;
          } else {
            othersCount += (count || 0);
          }
        });
      }
      if (hasOthers) row['Others'] = othersCount;
      return row;
    });

    return { failReasonChartData: formattedData, failReasonKeys: keys };
  }, [processedData]);


  // --- 4. Render (Integration) ---
  return (
    <div className="min-h-screen min-w-screen p-8 bg-gray-950 font-sans text-gray-100">
      <div className="max-w-7xl mx-auto">
        
        {/* Header Section */}
        <DashboardHeader timeFrame={timeFrame} setTimeFrame={setTimeFrame} />

        {/* 1. Health Status Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
          <StatusCardContainer name="Scheduler Server" url={CRAWLER_URL} urlName="http://ws2.csie.ntu.edu.tw:22225" />
          <StatusCardContainer name="TypeSense Server" url={TYPESENSE_URL} urlName="http://ws2.csie.ntu.edu.tw:22222" />
        </div>

        {/* 2. Total Volume Chart */}
        <div className="mb-6">
          <TotalVolumeChart data={processedData} />
        </div>

        {/* 3. Algorithm Metrics Section (Moved Up) */}
        <div className="mb-12">
            <h2 className="text-xl font-bold text-gray-200 mb-4 mt-8 border-l-4 border-purple-500 pl-3">
            Algorithm Performance Metrics
            </h2>
            
            <div className="grid grid-cols-1 gap-6">
            {headMetrics.length > 0 && (
                <MetricTrendChart 
                title="Head Strategy Performance (Daily)" 
                data={headMetrics} 
                />
            )}
            
            {randomMetrics.length > 0 && (
                <MetricTrendChart 
                title="Random Strategy Performance (Daily)" 
                data={randomMetrics} 
                />
            )}

            {headMetrics.length === 0 && randomMetrics.length === 0 && (
                <div className="p-8 text-center bg-gray-900 border border-gray-800 rounded-xl text-gray-500">
                Loading metrics data...
                </div>
            )}
            </div>
        </div>

        {/* 4. Daily Trends & Fail Reasons (Moved Down) */}
        <h2 className="text-xl font-bold text-gray-200 mb-4 border-l-4 border-blue-500 pl-3">
          Detailed Crawl Statistics
        </h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <DailyTrendsChart data={processedData} />
          <FailReasonsChart chartData={failReasonChartData} keys={failReasonKeys} />
        </div>

      </div>
    </div>
  );
}