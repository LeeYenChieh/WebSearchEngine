export interface FailureReasons {
    [key: string]: number;
}

export interface DataEntry {
    discovered: number;
    crawled: number;
    indexed: number;
    detail: {
        new_links: number;
        fetch_fail: number;
        fetch_ok: number;
        failed_rate: number;
        fail_reasons: FailureReasons;
    };
}

export interface RawData {
  [date: string]: DataEntry;
}

export interface ServerDailyStat {
    stat_date: string;
    displayDate: string;
    discovered: number;
    crawled: number;
    indexed: number;
    new_links: number;
    fetch_ok: number;
    fetch_fail: number;
    failed_rate: number;
    fail_reasons?: FailureReasons;
}

export type TimeFrame = 'daily' | 'weekly' | 'monthly';

export type StatusType = 'loading' | 'healthy' | 'unhealthy';

// 專門給 FailReasonsChart 用的 UI 資料格式
export interface FailReasonChartData {
    displayDate: string;
    [key: string]: string | number;
}
