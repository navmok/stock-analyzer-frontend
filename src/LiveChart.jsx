import { useEffect, useMemo, useRef } from "react";
import { createChart, CrosshairMode, CandlestickSeries } from "lightweight-charts";

// expects candles: [{ time: unixSeconds, open, high, low, close, volume }]
export default function LiveChart({ candles, height = 360 }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volSeriesRef = useRef(null);

  const data = useMemo(() => {
    return (candles || [])
      .map((c) => ({
        time: c.time,
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close),
        volume: Number(c.volume ?? 0),
      }))
      .sort((a, b) => a.time - b.time);
  }, [candles]);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      height,
      layout: {
        background: { color: "transparent" },
        textColor: "#e5e7eb",
        fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial",
      },
      grid: {
        vertLines: { color: "#1f2937" },
        horzLines: { color: "#1f2937" },
      },
      rightPriceScale: { borderColor: "#334155" },
      timeScale: { borderColor: "#334155", timeVisible: true, secondsVisible: false },
      crosshair: { mode: CrosshairMode.Normal },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { mouseWheel: true, pinch: true },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: "#22c55e",
        downColor: "#ef4444",
        borderUpColor: "#22c55e",
        borderDownColor: "#ef4444",
        wickUpColor: "#22c55e",
        wickDownColor: "#ef4444",
        });

    const volSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volSeriesRef.current = volSeries;

    const onResize = () => {
      if (!containerRef.current) return;
      chart.applyOptions({ width: containerRef.current.clientWidth });
    };
    window.addEventListener("resize", onResize);
    onResize();

    return () => {
      window.removeEventListener("resize", onResize);
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volSeriesRef.current = null;
    };
  }, [height]);

  // set full series
  useEffect(() => {
    if (!candleSeriesRef.current || !volSeriesRef.current) return;

    candleSeriesRef.current.setData(data);

    volSeriesRef.current.setData(
      data.map((d) => ({
        time: d.time,
        value: d.volume,
        // green/red volume bars
        color: d.close >= d.open ? "rgba(34,197,94,0.6)" : "rgba(239,68,68,0.6)",
      }))
    );

    if (chartRef.current) chartRef.current.timeScale().fitContent();
  }, [data]);

  // smooth “live” update on the last candle
  useEffect(() => {
    if (!candleSeriesRef.current || !volSeriesRef.current) return;
    if (!data.length) return;

    const last = data[data.length - 1];

    candleSeriesRef.current.update(last);
    volSeriesRef.current.update({
      time: last.time,
      value: last.volume,
      color: last.close >= last.open ? "rgba(34,197,94,0.6)" : "rgba(239,68,68,0.6)",
    });
  }, [data]);

  return <div ref={containerRef} style={{ width: "100%" }} />;
}
